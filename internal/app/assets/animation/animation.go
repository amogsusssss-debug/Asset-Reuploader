package animation

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartFr/Asset-Reuploader/internal/app/assets/shared/assetutils"
	"github.com/kartFr/Asset-Reuploader/internal/app/assets/shared/clientutils"
	"github.com/kartFr/Asset-Reuploader/internal/app/assets/shared/uploaderror"
	"github.com/kartFr/Asset-Reuploader/internal/app/context"
	"github.com/kartFr/Asset-Reuploader/internal/app/request"
	"github.com/kartFr/Asset-Reuploader/internal/app/response"
	"github.com/kartFr/Asset-Reuploader/internal/atomicarray"
	"github.com/kartFr/Asset-Reuploader/internal/retry"
	"github.com/kartFr/Asset-Reuploader/internal/roblox/assetdelivery"
	"github.com/kartFr/Asset-Reuploader/internal/roblox/develop"
	"github.com/kartFr/Asset-Reuploader/internal/roblox/games"
	"github.com/kartFr/Asset-Reuploader/internal/roblox/ide"
	"github.com/kartFr/Asset-Reuploader/internal/shardedmap"
	"github.com/kartFr/Asset-Reuploader/internal/taskqueue"
)

const assetTypeID int32 = 24
const animationUploadRetryTries = 3
const animationUploadRateLimitMaxPower = 6

var ErrUnauthorized = errors.New("authentication required to access asset")

// TokenBucket implements a token bucket rate limiter
type TokenBucket struct {
	tokens        float64
	maxTokens     float64
	refillRate    float64 // tokens per second
	lastRefillTime time.Time
	mu            sync.Mutex
}

// NewTokenBucket creates a new token bucket with the given capacity and refill rate
func NewTokenBucket(capacity float64, requestsPerSecond float64) *TokenBucket {
	return &TokenBucket{
		tokens:         capacity,
		maxTokens:      capacity,
		refillRate:     requestsPerSecond,
		lastRefillTime: time.Now(),
	}
}

// Wait blocks until a token is available and consumes it
func (tb *TokenBucket) Wait() {
	for {
		tb.mu.Lock()
		now := time.Now()
		
		// Refill tokens based on time elapsed
		elapsed := now.Sub(tb.lastRefillTime).Seconds()
		tb.tokens = math.Min(tb.maxTokens, tb.tokens+elapsed*tb.refillRate)
		tb.lastRefillTime = now
		
		// If token available, consume it and return
		if tb.tokens >= 1.0 {
			tb.tokens -= 1.0
			tb.mu.Unlock()
			return
		}
		
		tb.mu.Unlock()
		
		// Wait a bit before trying again (100ms)
		time.Sleep(100 * time.Millisecond)
	}
}

func animationRateLimitBackoff(try int) time.Duration {
	if try < 1 {
		try = 1
	}
	if try > animationUploadRateLimitMaxPower {
		try = animationUploadRateLimitMaxPower
	}
	return time.Minute * time.Duration(1<<(try-1))
}

func MoveValueToTop[T comparable](arr *atomicarray.AtomicArray[T], value T) {
	arr.Update(func(currentArray []T) []T {
		if currentArray[0] == value {
			return nil
		}

		for i, v := range currentArray {
			if v != value {
				continue
			}
			if i == 1 {
				currentArray[0], currentArray[1] = currentArray[1], currentArray[0]
				return currentArray
			}

			copy(currentArray[1:i+1], currentArray[0:i])
			currentArray[0] = value
			return currentArray
		}

		return nil
	})
}

func Reupload(ctx *context.Context, r *request.Request) {
	client := ctx.Client
	logger := ctx.Logger
	pauseController := ctx.PauseController
	resp := ctx.Response

	idsToUpload := len(r.IDs)
	var idsProcessed atomic.Int32

	defaultPlaceIDs := r.DefaultPlaceIDs
	defaultPlaceIDsMap := make(map[int64]struct{}, len(defaultPlaceIDs))
	for _, placeID := range defaultPlaceIDs {
		defaultPlaceIDsMap[placeID] = struct{}{}
	}

	var groupID int64
	if r.IsGroup {
		groupID = r.CreatorID
	}

	filter := assetutils.NewFilter(ctx, r, assetTypeID)

	creatorPlaceMap := shardedmap.New[*atomicarray.AtomicArray[int64]]()
	creatorMutexMap := shardedmap.New[*sync.RWMutex]()

	uploadQueue := taskqueue.New[int64](time.Minute, 3000)
	groupGameQueue := taskqueue.New[*games.GamesResponse](time.Second*5, 5)
	userGameQueue := taskqueue.New[*games.GamesResponse](time.Second*5, 5)

	// Anti-API rate limiter: 1 request per 2 seconds (30 per minute)
	// This is conservative enough to avoid Roblox rate limits
	antiRateLimiter := NewTokenBucket(1, 0.5) // 0.5 requests per second = 1 req per 2 seconds

	logger.Println("Reuploading animations...")

	newBatchError := func(amt int, m string, err any) {
		end := int(idsProcessed.Add(int32(amt)))
		start := end - amt
		logger.Error(uploaderror.NewBatch(start, end, idsToUpload, m, err))
	}

	newUploadError := func(m string, assetInfo *develop.AssetInfo, err any) {
		newValue := idsProcessed.Add(1)
		logger.Error(uploaderror.New(int(newValue), idsToUpload, m, assetInfo, err))
	}

	uploadAsset := func(wg *sync.WaitGroup, assetInfo *develop.AssetInfo, location string) {
		defer wg.Done()

		// Apply anti-API rate limiter BEFORE attempting upload
		antiRateLimiter.Wait()

		oldName := assetInfo.Name

		assetData, err := clientutils.GetRequest(client, location)
		if err != nil {
			newUploadError("Failed to get asset data", assetInfo, err)
			return
		}

		uploadHandler, err := ide.NewUploadAnimationHandler(client, assetInfo.Name, "", assetData, groupID)
		if err != nil {
			newUploadError("Failed to get upload handler", assetInfo, err)
			return
		}

		var finalErr error

		for try := 1; try <= animationUploadRetryTries; try++ {
			pauseController.WaitIfPaused()

			id, err := uploadHandler()
			if err == nil {
				newValue := idsProcessed.Add(1)
				logger.Success(uploaderror.New(int(newValue), idsToUpload, "", assetInfo, id))
				resp.AddItem(response.ResponseItem{
					OldID: assetInfo.ID,
					NewID: id,
				})
				return
			}

			finalErr = err

			switch err {
			case ide.UploadAnimationErrors.ErrNotLoggedIn:
				clientutils.GetNewCookie(ctx, r, "cookie expired")
				continue
			case ide.UploadAnimationErrors.ErrInappropriateName:
				assetInfo.Name = fmt.Sprintf("(%s) [Censored]", assetInfo.Name)
				continue
			default:
				// Always sleep on rate limit errors with exponential backoff
				if errors.Is(err, ide.ErrRateLimited) {
					backoffDuration := animationRateLimitBackoff(try)
					logger.Printf("Rate limited on try %d, waiting %v before retry", try, backoffDuration)
					time.Sleep(backoffDuration)
					continue
				}

				// Network errors: continue retry with small delay
				switch err.(type) {
				case *net.OpError, *net.DNSError:
					logger.Printf("Network error on try %d: %v, retrying...", try, err)
					time.Sleep(time.Second * time.Duration(try))
					continue
				}

				// Other errors: stop retrying
				break
			}
		}

		// Upload failed after all retries
		assetInfo.Name = oldName
		newUploadError("Failed to upload", assetInfo, finalErr)
	}

	getCreatorPlaceCache := func(creatorID int64, creatorType string) (*atomicarray.AtomicArray[int64], error) {
		creatorShard, exists := creatorPlaceMap.GetShard(creatorType)
		mutexShard, _ := creatorMutexMap.GetShard(creatorType)
		if !exists {
			creatorShard = creatorPlaceMap.NewShard(creatorType)
			mutexShard = creatorMutexMap.NewShard(creatorType)
		}

		if cache, cacheExists := creatorShard.Get(creatorID); cacheExists {
			return cache, nil
		}

		mutex, mutexExists := mutexShard.Get(creatorID)
		if !mutexExists {
			mutex = &sync.RWMutex{}
			mutexShard.Set(creatorID, mutex)
		}

		mutex.Lock()
		defer mutex.Unlock()

		if cache, cacheExists := creatorShard.Get(creatorID); cacheExists {
			return cache, nil
		}

		var resp *games.GamesResponse
		var err error
		if creatorType == "Group" {
			queueRes := <-groupGameQueue.QueueTask(func() (*games.GamesResponse, error) {
				return games.GroupGames(client, creatorID)
			})
			resp = queueRes.Result
			err = queueRes.Error
		} else {
			queueRes := <-userGameQueue.QueueTask(func() (*games.GamesResponse, error) {
				return games.UserGames(client, creatorID)
			})
			resp = queueRes.Result
			err = queueRes.Error
		}
		if err != nil {
			return nil, err
		}

		ids := make([]int64, 0, len(defaultPlaceIDs))
		for _, placeInfo := range resp.Data {
			rootPlaceID := placeInfo.RootPlace.ID

			if _, exists := defaultPlaceIDsMap[rootPlaceID]; exists {
				continue
			}
			ids = append(ids, rootPlaceID)
		}
		ids = append(ids, defaultPlaceIDs...)

		cache := atomicarray.New(&ids)
		creatorShard.Set(creatorID, cache)
		mutexShard.Remove(creatorID)
		return cache, nil
	}

	getAssetLocations := func(body []*assetdelivery.AssetRequestItem, placeID int64) ([]*assetdelivery.AssetLocation, error) {
		handler, err := assetdelivery.NewBatchHandler(client, body, placeID)
		if err != nil {
			return nil, err
		}

		return retry.Do(
			retry.NewOptions(retry.Tries(3)),
			func(try int) ([]*assetdelivery.AssetLocation, error) {
				pauseController.WaitIfPaused()

				locations, err := handler()
				if err != nil {
					return locations, &retry.ContinueRetry{Err: err}
				}

				for _, assetLocation := range locations {
					errs := assetLocation.Errors
					if errs == nil {
						continue
					}
					if errs[0].Message == "Authentication required to access Asset." {
						clientutils.GetNewCookie(ctx, r, "cookie expired")
						return locations, &retry.ContinueRetry{Err: ErrUnauthorized}
					}
				}

				return locations, nil
			},
		)
	}

	batchUpload := func(wg *sync.WaitGroup, creatorID int64, creatorType string, creatorAssets []*develop.AssetInfo) {
		defer wg.Done()

		placeCache, err := getCreatorPlaceCache(creatorID, creatorType)
		if err != nil {
			newBatchError(len(creatorAssets), "Failed to get creator places", err)
		}

		assetInfoMap := make(map[int64]*develop.AssetInfo)
		ids := make([]int64, len(creatorAssets))
		for i, assetInfo := range creatorAssets {
			ids[i] = assetInfo.ID
			assetInfoMap[assetInfo.ID] = assetInfo
		}
		body := assetutils.NewBatchBodyFromIDs(ids)

		var uploadWG sync.WaitGroup
		var assetLocations []*assetdelivery.AssetLocation
		creatorPlaceCache := placeCache.Load()
		for _, placeID := range creatorPlaceCache {
			assetLocations, err = getAssetLocations(body, placeID)
			if err != nil {
				newBatchError(len(body), "Failed to get asset locations", err)
				return
			}

			var hadSuccess bool
			for assetIndex, assetLocation := range slices.Backward(assetLocations) {
				if len(assetLocation.Locations) == 0 {
					continue
				}
				hadSuccess = true

				assetID := body[assetIndex].AssetID
				body = slices.Delete(body, assetIndex, assetIndex+1)

				uploadWG.Add(1)
				go uploadAsset(&uploadWG, assetInfoMap[assetID], assetLocation.Locations[0].Location)
			}
			if hadSuccess && len(creatorPlaceCache) > 1 {
				MoveValueToTop(placeCache, placeID)
			}
			if len(body) == 0 {
				break
			}
		}

		var index int
		for _, assetLocation := range assetLocations {
			if len(assetLocation.Locations) != 0 {
				continue
			}
			assetID := body[index].AssetID
			index++

			assetInfo := assetInfoMap[assetID]
			newUploadError("Failed to get asset location", assetInfo, assetLocation.Errors[0].Message)
		}

		uploadWG.Wait()
	}

	batchProcess := func(wg *sync.WaitGroup, res assetutils.AssetsInfoResult, batchSize int) {
		defer wg.Done()
		assetsInfo := res.Result

		if err := res.Error; err != nil {
			newBatchError(batchSize, "Failed to get assets info", err)
			return
		}

		filteredInfo := filter(assetsInfo)
		filteredInfoLength := len(filteredInfo)
		idsProcessed.Add(int32(batchSize - filteredInfoLength))
		if filteredInfoLength == 0 {
			return
		}

		CreatorAssets := make(map[string]map[int64][]*develop.AssetInfo)
		for _, assetInfo := range filteredInfo {
			assetCreatorType := assetInfo.Creator.Type
			assetCreatorID := assetInfo.Creator.TargetID

			creatorType, exists := CreatorAssets[assetCreatorType]
			if !exists {
				creatorType = make(map[int64][]*develop.AssetInfo)
				CreatorAssets[assetCreatorType] = creatorType
			}

			creatorAssets, exists := creatorType[assetCreatorID]
			if !exists {
				creatorAssets = make([]*develop.AssetInfo, 0)
				creatorType[assetCreatorID] = creatorAssets
			}

			creatorType[assetCreatorID] = append(creatorAssets, assetInfo)
		}

		var uploadWG sync.WaitGroup
		for creatorType, creatorAssetMap := range CreatorAssets {
			uploadWG.Add(len(creatorAssetMap))

			for creatorID, creatorAssets := range creatorAssetMap {
				go batchUpload(&uploadWG, creatorID, creatorType, creatorAssets)
			}
		}
		uploadWG.Wait()
	}

	var wg sync.WaitGroup
	tasks := assetutils.GetAssetsInfoInChunks(ctx, r)
	wg.Add(len(tasks))
	for i, task := range tasks {
		batchSize := 50
		if i == len(tasks)-1 {
			batchSize = idsToUpload % 50
			if batchSize == 0 {
				batchSize = 50
			}
		}

		go batchProcess(&wg, <-task, batchSize)
	}
	wg.Wait()
}
