local DataStoreService = game:GetService("DataStoreService")
local HttpService = game:GetService("HttpService")

local DataStore = DataStoreService:GetDataStore("Choose_A_Side")

local LOCKMAXTIME = 100 --constant
local JobId = tostring(game.JobId or "") 

if JobId == "" then
	JobId = HttpService:GenerateGUID(false)
end

local DataHandler = {}
DataHandler.__index = DataHandler

local function now()  --helper function for getting the time needed for checking if the lock is exipired or not
	local currentTime = os.time()
	return currentTime
end

local function mergeDefaults(dest, source)   --basically combines new or loaded data with the default set of values. Good for preventing data corruption
	if type(source) ~= "table" then
		return dest
	end

	for k, v in pairs(source) do
		local destValue = dest[k]

		if destValue == nil then
			if type(v) == "table" then
				local copy = {}

				for kk, vv in pairs(v) do
					copy[kk] = vv
				end

				dest[k] = copy
			else
				dest[k] = v
			end
		else
			local isDestTable = type(destValue) == "table"
			local isSourceTable = type(v) == "table"

			if isDestTable and isSourceTable then
				mergeDefaults(destValue, v)
			end
		end
	end

	return dest
end

local function SaveJobIdAndTime(self, DataKey) --atomically claims session lock, using the jobid 
	local attempts = 0
	local maxAttempts = 5
	local backoff = 0.4

	while attempts < maxAttempts do -- multiple attempts for safety 
		attempts = attempts + 1

		local ok, err = pcall(function()
			local transformFunction = function(oldData)
				oldData = oldData or {} -- "{}"handles new players with no data

				local oldSaved = tostring(oldData.SavedJobID or "0") 
				local oldTime = tonumber(oldData.TimeOfTheSave) or 0
				local current = now()

				local isUnlocked = oldSaved == "0" --checks if data is free	
				local timeDiff = current - oldTime
				local isExpired = timeDiff > LOCKMAXTIME -- This checks if previous lock expired

				if isUnlocked or isExpired then
					mergeDefaults(self.Data, oldData) --loads old data into a local table to prevent data loss while claiming the lock

					self.Data.SavedJobID = JobId
					self.Data.TimeOfTheSave = current

					return self.Data
				else
					return oldData --if another server has the lock it cancels the update
				end
			end

			DataStore:UpdateAsync(
				DataKey, 
				transformFunction
			)
		end)

		if not ok then
			warn("SaveJobIdAndTime UpdateAsync failed")
		else
			local okGet, currentData = pcall(function()
				return DataStore:GetAsync(DataKey)
			end)

			if okGet then
				if currentData then
					local savedID = tostring(currentData.SavedJobID or "0")

					if savedID == JobId then -- confirms this server won the race
						return true
					end
				end
			end
		end

		task.wait(backoff)

		backoff = backoff * 2 --just for reducing requests
		if backoff > 5 then
			backoff = 5
		end
	end

	warn(
		"Lock not claimed", 
		DataKey
	)

	return false
end

function DataHandler.new(player) --creates data handler
	local self = setmetatable(
		{}, 
		DataHandler
	)

	self.Player = player

	self.Data = {
		SavedJobID = "0",
		TimeOfTheSave = 0,
		Cash = 0,
		Victories = 0,
		Kills = 0,
		EquippedWeapon = 0,
		Weapons = {}
	}

	return self
end

function DataHandler:Save(DataKey) --saves the final data 
	local attempt = 0
	local maxAttempts = 5
	local backoff = 1

	while attempt < maxAttempts do --again multiple attempts for safety
		attempt = attempt + 1

		local success, err = pcall(function()
			local transformFunction = function(oldData)
				oldData = oldData or {}

				local oldSaved = tostring(oldData.SavedJobID or "0")

				if oldSaved == JobId then --checks if the server owns the lock
					self.Data.SavedJobID = "0" --releases the lock
					self.Data.TimeOfTheSave = now()

					return self.Data
				else
					return oldData -- stops if the lock is lost
				end
			end

			DataStore:UpdateAsync(
				DataKey, 
				transformFunction
			)
		end)

		if success then
			local okGet, currentData = pcall(function()
				return DataStore:GetAsync(DataKey)
			end)

			if okGet and currentData then
				local savedID = tostring(currentData.SavedJobID or "0")

				if savedID == "0" then
					return true
				end
			else
				local dataString = tostring(currentData)
				warn(
					"GetAsync verification failed", 
					dataString
				)
				return true
			end
		else
			warn("Save UpdateAsync failed")
		end

		task.wait(backoff)

		backoff = backoff * 2
		if backoff > 8 then
			backoff = 8
		end
	end

	return false
end

function DataHandler:Load(Player, DataKey)
	local ReturnedData
	local currentTime = now()

	local success, err = pcall(function()
		ReturnedData = DataStore:GetAsync(DataKey)
	end)

	if not success then
		local errorString = tostring(err)
		warn(
			"Failed to load data:", 
			errorString
		)

		Player:Kick("Data loading failed. Please rejoin.")
		return
	end

	if ReturnedData then
		print("Loaded successful")
	end

	local savedIdStr
	local savedTime

	if ReturnedData then
		savedIdStr = tostring(ReturnedData.SavedJobID)
	else
		savedIdStr = "0"
	end

	if ReturnedData then
		savedTime = tonumber(ReturnedData.TimeOfTheSave)
	else
		savedTime = 0
	end

	if not savedTime then
		savedTime = 0
	end

	local isUnlocked = false

	if ReturnedData then
		if savedIdStr == "0" then
			isUnlocked = true
		end
	else
		isUnlocked = true 
	end

	if isUnlocked then -- applies saved data and claims the lock
		if ReturnedData then
			mergeDefaults(self.Data, ReturnedData)
		end
		SaveJobIdAndTime(self, DataKey)
		return
	end

	if ReturnedData then
		if savedIdStr ~= "0" then
			local timeDiff = currentTime - savedTime

			if timeDiff < LOCKMAXTIME then
				Player:Kick("Multiple sessions detected. Please close other sessions and rejoin.")
				return
			end
		end
	end

	if ReturnedData then
		if savedIdStr ~= "0" then
			local timeDiff = currentTime - savedTime

			if timeDiff > LOCKMAXTIME then
				mergeDefaults(self.Data, ReturnedData)
				SaveJobIdAndTime(self, DataKey) -- forcefully claims the lock if previous server crashed

				return
			end
		end
	end

	SaveJobIdAndTime(self, DataKey)
end

return DataHandler
