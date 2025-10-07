# Summary of the state of the code
## Initial caveats
Apologies for the lateness of this work, a rather hectic weekend was paired with me trying to utilise a chromebook I have for the development which severly increased the difficulty (I was unable for example to get anything other than the VSCode SQLViewer working for interrogating the SQLite Dbs).
Anyway, things to note:
* have left formatting of financial values
* The use of ROWID as surrogate keys is unpleasant
* I think I might have missed some complxity with the linked loans, but have propagated the fk to PK join
* I have made the assumption that the client_contract spend threshold is a threshold after which the favourable discounted fee is applied. However if applied to the calculated running total (where refunds are negative and payments are positive) no client exceds the rate
* I have included the calculations for fees and amounts in a couple of different flavours (hoping one might be right):
* * Just the unsigned value of fee and transaction, the signed value of the fee and transaction (for debit or credit - prefixed with "accounting") and the same repeated once GBP conversion has occured (suffixed "gbp")
* I have chosen not to fail the models if there are missing currency_rates instead the rate of the previous existing rate is used and the record is flagged.

## Larger issues
* The fct_transaction table is wrong as for some reason (despite having formatted the date column in the staging layer) there appears a mismatch in date format between the transaction_date and resolution_date which means chargebacks are getting a null currency_rate.
* I can't get dbt/SQLite to handle incremental unqiue keys properly, so the first run will work (for the currency_rates table)...