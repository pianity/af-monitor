local schemas = require('schemas')
local json = require('json')

local insert = {}

function insert.ammTransaction(msg, source, sourceAmm)
    local valid, err = schemas.ammInputMessageSchema(msg)
    assert(valid, 'Invalid input transaction data' .. json.encode(err))

    local stmt = db:prepare [[
      REPLACE INTO amm_transactions (
        id, source, block_height, block_id, sender, created_at_ts,
        to_token, from_token, from_quantity, to_quantity, fee,
        amm_process, reserves_0, reserves_1
      ) VALUES (
        :id, :source, :block_height, :block_id, :sender, :created_at_ts,
        :to_token, :from_token, :from_quantity, :to_quantity, :fee,
        :amm_process, :reserves_0, :reserves_1
      );
    ]]
    if not stmt then
        error("Failed to prepare SQL statement: " .. db:errmsg())
    end
    stmt:bind_names({
        id = msg.Id,
        source = source,
        block_height = msg['Block-Height'],
        block_id = msg['Block-Id'] or '',
        sender = msg.recipient or '',
        created_at_ts = msg.Timestamp,
        to_token = msg.Tags['To-Token'],
        from_token = msg.Tags['From-Token'],
        from_quantity = tonumber(msg.Tags['From-Quantity']),
        to_quantity = tonumber(msg.Tags['To-Quantity']),
        fee = tonumber(msg.Tags['Fee']),
        amm_process = sourceAmm,
        reserves_0 = tonumber(msg.Tags['Reserve-Base']),
        reserves_1 = tonumber(msg.Tags['Reserve-Quote'])
    })

    stmt:step()
    stmt:reset()
end

function insert.dexOrder(msg, source)
    local valid, err = schemas.dexOrderMessageSchema(msg)
    assert(valid, 'Invalid input transaction data' .. json.encode(err))

    local stmt = db:prepare [[
      REPLACE INTO dex_orders (
        order_id, source, block_height, block_id, created_at_ts, updated_at_ts,
        type, status, original_quantity, executed_quantity, price, wallet,
        token_id, dex_process_id
      ) VALUES (
        :order_id, :source, :block_height, :block_id, :created_at_ts, :updated_at_ts,
        :type, :status, :original_quantity, :executed_quantity, :price, :wallet,
        :token_id, :dex_process_id
      );
    ]]
    if not stmt then
        print("Failed to prepare SQL statement: " .. db:errmsg())
    end
    stmt:bind_names({
        order_id = msg.Tags['Order-Id'] or msg.Tags['Pushed-For'] or msg.Id,
        source = source,
        block_height = msg['Block-Height'],
        block_id = msg['Block-Id'] or '',
        created_at_ts = msg.Timestamp,
        updated_at_ts = msg.Timestamp,
        type = string.upper(msg.Tags['Order-Type']),
        status = string.upper(msg.Tags['Order-Status']),
        original_quantity = msg.Tags['Original-Quantity'],
        executed_quantity = msg.Tags['Executed-Quantity'],
        price = msg.Tags['Price'],
        wallet = msg.Tags['Wallet'],
        token_id = msg.Tags['Token-Id'],
        dex_process_id = msg.Tags.From
    })
    stmt:step()
    stmt:reset()
    print('Order OK')
end

function insert.dexTrade(msg, source)
    local valid, err = schemas.dexTradeMessageSchema(msg)
    assert(valid, 'Invalid input transaction data' .. json.encode(err))

    local stmt = db:prepare [[
        REPLACE INTO dex_trades (
            trade_id, source, block_height, block_id, created_at_ts,
            quantity_base, quantity_quote, price, maker_fees, taker_fees,
            is_buyer_taker, maker_order_id, taker_order_id, dex_process_id
          ) VALUES (
            :trade_id, :source, :block_height, :block_id, :created_at_ts,
            :quantity_base, :quantity_quote, :price, :maker_fees, :taker_fees,
            :is_buyer_taker, :maker_order_id, :taker_order_id, :dex_process_id
          );
        ]]

    if not stmt then
        print("Failed to prepare SQL statement: " .. db:errmsg())
    end


    stmt:bind_names({
        trade_id = msg.Id,
        source = source,
        block_height = msg['Block-Height'],
        block_id = msg['Block-Id'] or '',
        created_at_ts = msg.Timestamp,
        quantity_base = msg.Tags['Quantity-Base'],
        quantity_quote = msg.Tags['Quantity-Quote'],
        price = msg.Tags['Price'],
        maker_fees = msg.Tags['Maker-Fees'],
        taker_fees = msg.Tags['Taker-Fees'],
        is_buyer_taker = msg.Tags['Is-Buyer-Taker'],
        maker_order_id = msg.Tags['Maker-Order-Id'],
        taker_order_id = msg.Tags['Taker-Order-Id'],
        dex_process_id = msg.Tags.From
    })
    stmt:step()
    stmt:reset()
    print('Trade OK')
end

return insert;
