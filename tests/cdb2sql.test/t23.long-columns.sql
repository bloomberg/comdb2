 put tunable return_long_column_names 0
 select 1 as 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'

 set return_long_column_names off
 select 1 as 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'
