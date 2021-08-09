from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    payload_pk = payload['sender_pk']
    if payload['platform'] == 'Algorand':
        return algosdk.util.verify_bytes(json.dumps(payload).encode('utf-8'), sig, payload_pk)
    else:
        eth_encoded_msg = eth_account.messages.encode_defunct(text=json.dumps(payload))
        return eth_account.Account.recover_message(eth_encoded_msg,signature=sig) == payload_pk
  
def fill_order(order):
    print("fill_order:", str(order))
    g.session.add(order)
    g.session.commit()
        
    #2.    Check if there are any existing orders that match. 
    orders = g.session.query(Order).filter(Order.filled == datetime(1, 1, 1, 0, 0)).all() #Get all unfilled orders
    for existing_order in orders:
      if (existing_order.buy_currency == order.sell_currency and 
        existing_order.sell_currency == order.buy_currency and 
        existing_order.sell_amount/existing_order.buy_amount >= order.buy_amount/order.sell_amount): #match
        print("matched")
    
        #3.    If a match is found between order and existing_order:
        #– Set the filled field to be the current timestamp on both orders
        dt = datetime.utcnow
        existing_order.filled = dt
        order.filled = dt
        
        #– Set counterparty_id to be the id of the other order
        existing_order.counterparty_id = order.id
        order.counterparty_id = existing_order.id

        #– If one of the orders is not completely filled (i.e. the counterparty’s sell_amount is less than buy_amount):
        if existing_order.sell_amount < order.buy_amount: #this order is not completely filled
          parent_order = order
          buy_amount = order.buy_amount - existing_order.sell_amount
          sell_amount = order.sell_amount - existing_order.buy_amount
          #print("parent_order = order")
          
        if order.sell_amount < existing_order.buy_amount: #existing_order is not completely filled
          parent_order = existing_order
          buy_amount = existing_order.buy_amount - order.sell_amount
          sell_amount = existing_order.sell_amount - order.buy_amount
          #print("parent_order = existing_order")
          
        if existing_order.sell_amount < order.buy_amount or order.sell_amount < existing_order.buy_amount:
          #print("parent_order is not None")
          #o    Create a new order for remaining balance
          child_order = {} #new dict
          child_order['buy_amount'] = buy_amount
          child_order['sell_amount'] = sell_amount
          child_order['buy_currency'] = parent_order.buy_currency
          child_order['sell_currency'] = parent_order.sell_currency
          
          #o    The new order should have the created_by field set to the id of its parent order
          child_order['creator_id'] = parent_order.id
          
          #o    The new order should have the same pk and platform as its parent order
          child_order['sender_pk'] = parent_order.sender_pk
          child_order['receiver_pk'] = parent_order.receiver_pk
          
          #o    The sell_amount of the new order can be any value such that the implied exchange rate of the new order is at least that of the old order
          #o    You can then try to fill the new order
          child_order['filled'] = datetime(1, 1, 1, 0, 0)
          corder = Order(**{f:child_order[f] for f in child_order})
          fill_order(corder)
          
          break
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    msg = json.dumps(d)
    log = Log(message = msg)
    g.session.add(log)
    g.session.commit()

""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session

        # TODO: Check the signature
        sig = content['sig']
        payload = content['payload']
        if check_sig(payload,sig): #If the signature verifies, store the signature, as well as all of the fields under the ‘payload’ in the “Order” table EXCEPT for 'platform'.
            # TODO: Add the order to the database
            print('signature does verify')
            del payload['platform']
            del payload['pk']
            payload['signature'] = sig
            payload['filled'] = datetime(1, 1, 1, 0, 0)
            order = Order(**{f:payload[f] for f in payload})
            # TODO: Fill the order
            fill_order(order)
            
            return jsonify(True) # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful

        else:  #If the signature does not verify, do not insert the order into the “Order” table. Instead, insert a record into the “Log” table, with the message field set to be json.dumps(payload).
            print('signature does not verify')
            log_message(payload)
            return jsonify(False) # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful

@app.route('/order_book')
def order_book():
    #Your code here
    #Note that you can access the database session using g.session
    l = []
    orders = g.session.query(Order).options(load_only("sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "signature")).all()
    for order in orders:
        d = {"sender_pk": order.sender_pk, "receiver_pk": order.receiver_pk, "buy_currency": order.buy_currency, "sell_currency": order.sell_currency, "buy_amount": order.buy_amount, "sell_amount": order.sell_amount, "signature": order.signature}
        #print("      ", d)
        l.append(d)
    result = {'data': l}
    return jsonify(result)

if __name__ == '__main__':
    app.run(port='5002')
