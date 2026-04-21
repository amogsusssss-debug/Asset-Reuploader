--!strict
local UiLibrary = require("../../UiLibrary")

return function(ui: UiLibrary.Ui, plugin: Plugin)
    local tab = ui:CreateTab("Settings")

    local split = tab:CreateSplit()
    local portInput; portInput = split:CreateInput("Port: " .. plugin:GetSetting("Port"), function(input)
        local newPort = tonumber(input)
        if not newPort then return end

        if newPort < 1024 or newPort > 49151 then
            ui:Notify("Notification", "Port has to be between 1024-49151.")
            return
        end

        plugin:SetSetting("Port", newPort)
        portInput.instance.TextBox.PlaceholderText = "Port: " .. newPort
    end)
    split:CreateToggle("Export json when done", function(state)
        plugin:SetSetting("ExportJSON", state)
    end, plugin:GetSetting("ExportJSON"))
end
