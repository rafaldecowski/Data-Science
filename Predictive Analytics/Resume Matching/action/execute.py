from datetime import datetime
from action.get_data import symbol_summary
from random import randint



def buy_shares(quantity, symbol, blotter, portfolio):
    ''' Takes user input (dropdown symbol selection and quantity) and executes the trade. '''

    # Get the latest price for the selected symbol
    share_price = float(symbol_summary(symbol)['last_price'])

    # Calculate the total value of this transaction
    transaction_value = float(share_price * quantity)

    # Retrieve the most recent cash value from blotter
    cash = float(blotter.find({}).sort("_id", -1).limit(1)[0]['Cash'])

    # Confirm available funds for this transaction
    if transaction_value > cash:
        message = "Insufficient Funds. You can afford " + str(round(cash / share_price, 2)) + str(symbol) + " shares."
        return message
    else:
        # Adding transaction to blotter
        new_cash = cash - transaction_value
        blotter.insert_one({'Date': datetime.now(),
                            'Price': share_price,
                            'Quantity': quantity,
                            'Side': 'Buy',
                            'Symbol': symbol,
                            'Value': transaction_value,
                            'Cash': new_cash})

        # Check if the symbol is in the portfolio and update its values
        if symbol in portfolio.index:

            # Save values prior to update
            current_inventory = portfolio.get_value(symbol, 'Inventory')
            current_WAP = portfolio.get_value(symbol, 'WAP')
            current_rpl = portfolio.get_value(symbol, 'RPL')

            # Recalculate WAP
            new_inventory = current_inventory + quantity
            new_wap = ((current_inventory * current_WAP) + transaction_value) / (new_inventory)

            # Recalculate UPL
            new_upl = (share_price - new_wap) * new_inventory

            # Insert into PL
            portfolio.set_value(symbol, 'Inventory', new_inventory)
            portfolio.set_value(symbol, 'Last Price', share_price)
            portfolio.set_value(symbol, 'WAP', new_wap)
            portfolio.set_value(symbol, 'UPL', new_upl)
            portfolio.set_value(symbol, 'Total PL', new_upl + current_rpl)

            message = "You successfully purchased " + str(quantity) + " " + str(symbol) + " shares worth " + str(transaction_value) + "."
            return message
        else:

            # Insert into PL
            portfolio.set_value(symbol, 'Inventory', quantity)
            portfolio.set_value(symbol, 'Last Price', share_price)
            portfolio.set_value(symbol, 'WAP', share_price)
            portfolio.set_value(symbol, 'UPL', 0)  # Since the symbol was just added, there is no UPL
            portfolio.set_value(symbol, 'RPL', 0)
            portfolio.set_value(symbol, 'Total PL', 0)
            message = "You successfully purchased " + str(quantity) + " " + str(symbol) + " shares worth " + str(transaction_value) + "."
            return message

def is_sufficient_inventory(quantity, symbol, portfolio):
    if portfolio.loc[symbol, 'Inventory'] >= quantity:
        return True
    else:
        return False


def sell_shares(quantity, symbol, blotter, portfolio):

    # Cannot sell more shares than owned
    if is_sufficient_inventory(quantity, symbol, portfolio):

        # Get the latest symbol price and calculate the value of this transaction
        share_price = float(symbol_summary(symbol)['last_price'])
        transaction_value = float(share_price * quantity)

        # Calculate Realized PL
        new_rpl = quantity * (share_price - portfolio.loc[symbol, 'WAP']) + portfolio.get_value(symbol, 'RPL')

        # Calculate the cash after the transaction
        cash = float(blotter.find({}).sort("_id", -1).limit(1)[0]['Cash'])
        new_cash = cash + transaction_value

        # Adding transaction to blotter
        blotter.insert_one({'Date': datetime.now(),
                            'Price': share_price,
                            'Quantity': quantity,
                            'Side': 'Sell',
                            'Symbol': symbol,
                            'Value': transaction_value,
                            'Cash': new_cash})

        # Save values prior to update
        current_inventory = portfolio.get_value(symbol, 'Inventory')
        new_inventory = current_inventory - quantity

        # Remove from portfolio if there are no shares
        if new_inventory == 0:
            portfolio.drop([symbol])
            message = "You successfully sold all of your " + str(symbol) + " shares."
            return message
        else:
            # Proceed to update the portfolio when there are some inventory for this symbol remaining
            current_WAP = portfolio.get_value(symbol, 'WAP')
            current_rpl = portfolio.get_value(symbol, 'RPL')

            # Recalculate WAP
            new_upl = new_inventory * (share_price - current_WAP)

            # Insert into PL
            portfolio.set_value(symbol, 'Inventory', new_inventory)
            portfolio.set_value(symbol, 'RPL', new_rpl)
            portfolio.set_value(symbol, 'UPL', new_upl)

            message = "You successfully sold " + str(quantity) + " " + str(symbol) + " shares worth " + str(transaction_value) + "."
            return message

    else:
        message = "Cannot sell more shares than owned. (" + str(portfolio.loc[symbol, 'Inventory']) + ")"
        return message


