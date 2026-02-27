schema change with live update:
(good case would be to update a record that is to the left of schemachange cursor)

schemachange's convert_records thread starts one thread per stripe to convert records in that stripe

block processor call upd_record()
                        |
                        |  (if has_constraints, we update genid in ct_add_table)                                       
                        |     For foreign key "key_i" -> <"p":"key_i">:
                        │        insert_add_op() -> Queues to ct_add_table
                        │        (stores: genid, table=t1, operation=UPDATE)
                        | 
                        |_ upd_record_indices() - Update key_i index in OLD table
                        |
                        |_ live_sc_post_update() - we fall into 4th case of live_sc_post_update_int docstring
                                                        |_ live_sc_post_upd_record()
                                                        |       (switch usedb)
                                                        |_ upd_new_record() - find rec in new table using oldgenid
                                                        |        (previously in upd_new_indices -> deferredAdd
                                                        |              if we don't do inline update,  
                                                        |          so we'll add to indices at the end)
                                                        |_ delayed_key_adds() (block processor calls this before commit)
                                                        |        (this adds defferedkeys to old table,
                                                        |              also adds to new table)
                                                        |_ verify_add_constraints() - checks insert/update FK constraints
