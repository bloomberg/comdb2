#ifndef _DB_CLNT_SETTINGS_H
#define _DB_CLNT_SETTINGS_H


// REGISTER_SETTING(NAME, DESC, TYPE, FLAG, DEFAULT)
REGISTER_ACC_SETTING(dbtran, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(dbtran.mode, "", SETTING_ENUM, SETFLAG_DERIVED, gbl_setting_default_query_timeout);

REGISTER_SETTING(dbtran.maxchunksize, "", SETTING_ENUM, SETFLAG_DERIVED, gbl_setting_default_query_timeout);

// How do I handle cln->plugin.set_timeout?
REGISTER_ACC_SETTING(plugin, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(plugin.set_timeout, "", SETTING_FUNC, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(query_timeout, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(tzname, "", SETTING_STRING, 0, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(dtprec, "", SETTING_INTEGER, 0, gbl_setting_default_query_timeout);

REGISTER_ACC_SETTING(current_user, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.name, "", SETTING_STRING, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.is_x509_user, "", SETTING_INTEGER, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_SETTING(current_user.have_password, "", SETTING_INTEGER, SETFLAG_DERIVED, gbl_setting_default_query_timeout);
REGISTER_ACC_SETTING(authgen, "", SETTING_COMPOSITE, 0, gbl_setting_default_query_timeout);

/**
spname
spversion.version_num
spversion.version_str

want_stored_procedure_trace
bdb_osql_trak
verifyretry_off,
stateement_query_effects,
get_cost,
is_explain
osql_max_trans
group_concat_mem_limit,
planner_effort,
appdata???? < - where did this come from?
admin = INT, ,
is_readonly, INT , MAIN
is_expert INT, MAIN
is_fast_expert DERIVED
*/

#endif
