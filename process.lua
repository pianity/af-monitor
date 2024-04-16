local json = require("json")


local intervals = require("intervals")
local candles = require "candles"
local stats = require "stats"
local schemas = require "schemas"
local sqlite3 = require("lsqlite3")
local sqlschema = require("sqlschema")

db = db or sqlite3.open_memory()

sqlschema.createTableIfNotExists(db)

--
OFFCHAIN_FEED_PROVIDER = 'iC5mu-_GkholDuxBrzI-rm1gIUagPrBOWhqzUwKBosk'
TOKEN = ao.env.Process.Tags["Base-Token"]
AMM =  ao.env.Process.Tags["Monitor-For"]

local function insertSingleMessage(msg, source, sourceAmm)
  local valid, err = schemas.inputMessageSchema(msg)
  assert(valid, 'Invalid input transaction data' .. json.encode(err))

  local stmt, err = db:prepare[[
    REPLACE INTO amm_transactions (
      id, source, block_height, block_id, sender, created_at_ts, 
      to_token, from_token, from_quantity, to_quantity, fee, amm_process
    ) VALUES (:id, :source, :block_height, :block_id, :sender, :created_at_ts, 
              :to_token, :from_token, :from_quantity, :to_quantity, :fee, :amm_process);  
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names({
    id = msg.Id,
    source = source,
    block_height = msg['Block-Height'],
    block_id = msg['Block-Id'] or '',
    sender = msg.From,
    created_at_ts = msg.Timestamp,
    to_token = msg.Tags['To-Token'],
    from_token = msg.Tags['From-Token'],
    from_quantity = tonumber(msg.Tags['From-Quantity']),
    to_quantity = tonumber(msg.Tags['To-Quantity']),
    fee = tonumber(msg.Tags['Fee']),
    amm_process = sourceAmm
  })

  stmt:step()
  stmt:reset()
end


function debugTable()
  local stmt = db:prepare[[
    SELECT * FROM amm_transactions ORDER BY created_at_ts LIMIT 100;
  ]]
  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end
  return sqlschema.queryMany(stmt)
end

local function findPriceAroundTimestamp(targetTimestampBefore, ammProcessId)
  local stmt = db:prepare[[
    SELECT price
    FROM amm_transactions_view
    WHERE created_at_ts <= :target_timestamp_before
    AND amm_process = :amm_process_id
    ORDER BY created_at_ts DESC
    LIMIT 1;
  ]]

  if not stmt then
    error("Failed to prepare SQL statement: " .. db:errmsg())
  end

  stmt:bind_names({
    target_timestamp_before = targetTimestampBefore,
    amm_process_id = ammProcessId
  })


  local row = sqlschema.queryOne(stmt)
  local price = row and row.price or nil

  return price
end


Handlers.add(
  "GetStats",
  Handlers.utils.hasMatchingTag("Action", "Get-Stats"),
  function (msg)
    local stats = stats.getAggregateStats(0, msg.Tags.AMM)
    local now = msg.Timestamp / 1000

    local priceNow = findPriceAroundTimestamp(now, msg.Tags.AMM)
    local price24HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1d'], msg.Tags.AMM)
    local price6HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['6h'], msg.Tags.AMM)
    local price1HAgo = findPriceAroundTimestamp(now - intervals.IntervalSecondsMap['1h'], msg.Tags.AMM)
    
    ao.send({
      Target = msg.From, 
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Stats',
      ['AMM'] = msg.Tags.AMM,
      ['Total-Volume'] = tostring(stats.total_volume),
      ['Buy-Volume'] = tostring(stats.buy_volume),
      ['Sell-Volume'] = tostring(stats.sell_volume),
      ['Buy-Count'] = tostring(stats.buy_count),
      ['Sell-Count'] = tostring(stats.sell_count),
      ['Buyers'] = tostring(stats.distinct_buyers),
      ['Sellers'] = tostring(stats.distinct_sellers),
      ['Total-Traders'] = tostring(stats.distinct_traders),
      ['Latest-Price'] = tostring(priceNow),
      ['Price-24H-Ago'] = tostring(price24HAgo),
      ['Price-6H-Ago'] = tostring(price6HAgo),
      ['Price-1H-Ago'] = tostring(price1HAgo)
    })
  end
)

-- Handlers.add(
--   "HandleDexiMessage",
--   Handlers.utils.hasMatchingTag("Action", "Dexi-Update"),
--   function (msg)
--     local prevMA50 = msg.Tags['Prev-MA-50']
--     local prevMA200 = msg.Tags['Prev-MA-200'] 
--     local currentMA50 = msg.Tags['Current-MA-50']
--     local currentMA200 = msg.Tags['Current-MA-200']

--     if prevMA50 <= prevMA200 and currentMA50 > currentMA200 then
--       executeSwap(...)
--     end
--   end
-- )



Handlers.add(
  "GetCandles",
  Handlers.utils.hasMatchingTag("Action", "Get-Candles"),
  function (msg)
    local days = msg.Tags.Days and tonumber(msg.Tags.Days) or 30
    local candles = candles.generateCandlesForXDaysInIntervalY(days, msg.Tags.Interval, msg.Timestamp / 1000, msg.Tags.AMM)
    ao.send({
      Target = msg.From,
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Candles',
      ['AMM'] = msg.Tags.AMM,
      ['Interval'] = msg.Tags.Interval or '15m',
      ['Days'] = tostring(msg.Tags.Days),
      Data = json.encode(candles)
    })
  end
)

Handlers.add(
  "UpdateLocalState", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Order-Confirmation-Monitor"),
  function (msg)
    local stmt = 'SELECT TRUE FROM amm_registry WHERE amm_process = :amm_process'
    local stmt = db:prepare(stmt)
    stmt:bind_names({ amm_process = msg.From })

    msg.Timestamp = math.floor(msg.Timestamp / 1000)
    local row = sqlschema.queryOne(stmt)
    if row or msg.From == Owner then
      insertSingleMessage(msg, 'message', msg.From)
    end
  end
)


Handlers.add(
  "GetRegisteredAMMs",
  Handlers.utils.hasMatchingTag("Action", "Get-Registered-AMMs"),
  function (msg)
    ao.send({
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Registered-AMMs',
      Target = msg.From,
      Data = json.encode(sqlschema.getRegisteredAMMs())
    })
  end
)

Handlers.add(
  "GetOverview", 
  Handlers.utils.hasMatchingTag("Action", "Get-Overview"),
  function (msg)
    local now = msg.Timestamp / 1000
    local orderBy = msg.Tags['Order-By']
    ao.send({
      ['App-Name'] = 'Dexi',
      ['Payload'] = 'Overview',
      Target = msg.From,
      Data = json.encode(sqlschema.getOverview(now, orderBy))
    })
  end
)


Handlers.add(
  "ReceiveOffchainFeed", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Receive-Offchain-Feed"),
  function (msg)
    if msg.From == OFFCHAIN_FEED_PROVIDER then
      local data = json.decode(msg.Data)
      for _, transaction in ipairs(data) do
        insertSingleMessage(transaction, 'gateway', transaction.Tags['AMM'])
      end
    end
  end
)


Handlers.add(
  "GetCurrentHeight",
  Handlers.utils.hasMatchingTag("Action", "Get-Current-Height"),
  function (msg)
    local stmt = db:prepare[[
      SELECT MAX(block_height) AS max_height
      FROM amm_transactions
      WHERE source = 'gateway' AND amm_process = :amm;
    ]]

    stmt:bind_names({ amm = msg.Tags.AMM})

    local row = sqlschema.queryOne(stmt)
    local gatewayHeight = row and row.max_height or 0

    stmt:reset()

    ao.send({
      Target = msg.From,
      Height = gatewayHeight
    })
  end
)


Handlers.add(
  "GetAMM", -- handler name
  Handlers.utils.hasMatchingTag("Action", "Get-AMM"), -- handler pattern to identify cron message
  function (msg)
    ao.send({
      Target = msg.From,
      AMM = AMM
    })
  end
)

Handlers.add(
  "Reset-Table",
  Handlers.utils.hasMatchingTag("Action", "Reset-Table"),
  function (msg)
    if msg.From == Owner then
      sqlschema.dropAndRecreateTableIfOwner(db)

      ao.send({
        Target = msg.From,
        Message = 'Reset OK'
      })
    end
  end
)

Handlers.add(
  "DumpTableToCSV",
  Handlers.utils.hasMatchingTag("Action", "Dump-Table-To-CSV"),
  function (msg)
    local stmt = db:prepare[[
      SELECT *
      FROM amm_transactions;
    ]]

    local rows = {}
    local row = stmt:step()
    while row do
      table.insert(rows, row)
      row = stmt:step()
    end

    stmt:reset()

    local csvHeader = "id,source,block_height,block_id,from,timestamp,is_buy,price,volume,to_token,from_token,from_quantity,to_quantity,fee,amm_process\n"
    local csvData = csvHeader

    for _, row in ipairs(rows) do
      local rowData = string.format("%s,%s,%d,%s,%s,%d,%d,%.8f,%.8f,%s,%s,%.8f,%.8f,%.8f,%s\n",
        row.id, row.source, row.block_height, row.block_id, row["from"], row["timestamp"],
        row.is_buy, row.price, row.volume, row.to_token, row.from_token, row.from_quantity,
        row.to_quantity, row.fee, row.amm_process)
      csvData = csvData .. rowData
    end

    ao.send({
      Target = msg.From,
      Data = csvData
    })
  end
)


-- Handlers.add(
--   "receive-data-feed",
--   Handlers.utils.hasMatchingTag("Action", "Receive-data-feed"),
--   function (msg)
--     local data = json.decode(msg.Data)
--     if data.data.transactions then
--       updateTransactions(data.data.transactions.edges)
--       print('transactions updated')
--       if #data.data.transactions.edges > 0 then
--         requestTransactions(100)
--       end
--       requestBlocks()
--     elseif data.data.blocks then
--       updateBlockTimestamps(data.data.blocks.edges)
--       print('blocks updated')
--     end
--   end
-- )