#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "comdb2_plugin.h"
#include "comdb2_initializer.h"
#include "sqlquery.pb-c.h"
#include "cdb2api_int.h"

#include "logmsg.h"

extern char gbl_dbname[];
extern int gbl_uses_simpleauth;
extern int gbl_uses_externalauth;
extern int gbl_allow_old_authn;
extern struct cdb2_identity *identity_cb;

int cdb2_in_client_trans();

int simpleAuthCheck(const char *principal, const char *verb, const char *resource);

typedef struct authenticationDataT {
    const char *principal;
    int major_version;
    int minor_version;
    int dta_size;
    void *dta;
    CDB2SQLQUERY__IdentityBlob *id;
} authenticationData;

static int simpleCheckTableAccess(void *ID, const char *tablename, const char *argv0, const char *action)
{
    char resource[512];
    int off;
    const char *principal;

    /* No identity blob sent - check if anonymous access is allowed */
    if (!ID) {
        principal = "bpi:anonymous";
    } else {
        authenticationData *authData = (authenticationData *)ID;
        principal = authData->principal;
    }

    off = snprintf(resource, sizeof(resource), "bri:comdb2:database:%s", gbl_dbname);
    if (tablename) {
        if (strncasecmp(tablename, "sqlite_", 7) == 0) {
            return 0;
        }
        int len = strlen(tablename);
        if (len >= 3 && tablename[len - 2] == '(' && tablename[len - 1] == ')') {
            snprintf(resource + off, sizeof(resource) - off, ":stored-procedure:%.*s", len - 2, tablename);
        } else {
            snprintf(resource + off, sizeof(resource) - off, ":table:%s", tablename);
        }
    }

    logmsg(LOGMSG_USER, "simpleauth: principal='%s' client_info='%s' action='%s'\n",
           principal, argv0 ? argv0 : "(null)", action);
    return simpleAuthCheck(principal, action, resource);
}

static int simpleCheckOPAccess(void *ID)
{
    return simpleCheckTableAccess(ID, NULL, NULL, "comdb2:OP");
}

static int simpleCheckTableAccessRead(void *ID, const char *tablename, const char *argv0)
{
    return simpleCheckTableAccess(ID, tablename, argv0, "comdb2:Read");
}

static int simpleCheckTableAccessWrite(void *ID, const char *tablename, const char *argv0)
{
    return simpleCheckTableAccess(ID, tablename, argv0, "comdb2:Write");
}

static int simpleCheckTableAccessDDL(void *ID, const char *tablename)
{
    return simpleCheckTableAccess(ID, tablename, NULL, "comdb2:DDL");
}

static int simpleCheckTableAccessMakeRequest(void *ID, const char *argv0)
{
    return simpleCheckTableAccess(ID, NULL, argv0, "comdb2:Connect");
}

static void *simpleNewsqlAuthData(void *buf, CDB2SQLQUERY__IdentityBlob *id)
{
    if (id->principal && id->data.data) {
        authenticationData *authData = NULL;
        if (buf) {
            authData = (authenticationData *)buf;
        } else {
            authData = (authenticationData *)malloc(sizeof(authenticationData));
            if (!authData)
                return NULL;
        }
        authData->principal = id->principal;
        authData->major_version = id->majorversion;
        authData->minor_version = id->minorversion;
        authData->dta_size = id->data.len;
        authData->dta = id->data.data;
        authData->id = id;
        return authData;
    }
    return NULL;
}

static void simpleFreeNewsqlAuthData(void *buf)
{
    if (buf) {
        free(buf);
    }
}

static void *simpleGetAuthIdBlob(void *ID)
{
    if (ID) {
        authenticationData *authData = (authenticationData *)ID;
        return authData->id;
    }
    return NULL;
}

static int simpleSerializeIdentity(void *ID, int *length, void **dta)
{
    authenticationData *authData = (authenticationData *)ID;
    *length = protobuf_c_message_get_packed_size((ProtobufCMessage *)authData->id);
    *dta = malloc(*length);
    if (!*dta)
        return -1;
    size_t len = protobuf_c_message_pack((ProtobufCMessage *)authData->id, (unsigned char *)*dta);
    if (len != *length)
        return -1;
    return 0;
}

static int simpleDeSerializeIdentity(void **ID, int length, unsigned char *dta)
{
    if (*ID != NULL) {
        authenticationData *id_data = (authenticationData *)*ID;
        protobuf_c_message_free_unpacked((ProtobufCMessage *)id_data->id, NULL);
        id_data->id = NULL;
        free(id_data);
        *ID = NULL;
    }
    if (dta) {
        CDB2SQLQUERY__IdentityBlob *id = (CDB2SQLQUERY__IdentityBlob *)protobuf_c_message_unpack(
            (ProtobufCMessageDescriptor *)&cdb2__sqlquery__identity_blob__descriptor, NULL, length, dta);
        if (id)
            *ID = simpleNewsqlAuthData(NULL, id);
        else
            return -1;
    }
    return 0;
}

extern int (*externalComdb2AuthenticateUserRead)(void *, const char *tablename, const char *argv0);
extern int (*externalComdb2AuthenticateUserWrite)(void *, const char *tablename, const char *argv0);
extern int (*externalComdb2AuthenticateUserDDL)(void *, const char *tablename);
extern int (*externalComdb2AuthenticateUserMakeRequest)(void *, const char *argv0);

extern int (*externalComdb2CheckOpAccess)(void *);
extern void *(*externalMakeNewsqlAuthData)(void *, CDB2SQLQUERY__IdentityBlob *id);
extern void (*externalFreeNewsqlAuthData)(void *);

extern int (*externalComdb2DeSerializeIdentity)(void **ID, int length, unsigned char *dta);
extern int (*externalComdb2SerializeIdentity)(void *ID, int *length, void **dta);
extern void *(*externalComdb2getAuthIdBlob)(void *ID);

/* Server-side identity callbacks for cdb2api outbound connections.
 * When simpleauth is enabled, the comdb2 process identifies itself as
 * bpi:procauth:cluster:<cluster>:user:op:bpkg:comdb2 on outbound connections
 * (e.g. bulkimport connecting to the source DB). */

const char *get_my_mach_cluster(void);

static CDB2SQLQUERY__IdentityBlob *simpleauth_id_blob = NULL;

static void simpleauth_identity_create(void)
{
    if (simpleauth_id_blob)
        return;

    const char *cluster = get_my_mach_cluster();
    if (!cluster)
        cluster = "default";

    CDB2SQLQUERY__IdentityBlob *id = calloc(1, sizeof(CDB2SQLQUERY__IdentityBlob));
    if (!id)
        return;
    cdb2__sqlquery__identity_blob__init(id);

    const int sz = snprintf(NULL, 0, "bpi:procauth:cluster:%s:user:op:bpkg:comdb2", cluster) + 1;
    id->principal = malloc(sz);
    if (!id->principal) {
        free(id);
        return;
    }
    snprintf(id->principal, sz, "bpi:procauth:cluster:%s:user:op:bpkg:comdb2", cluster);

    id->majorversion = 1;
    id->minorversion = 0;
    id->data.data = (uint8_t *)strdup("simpleauth");
    id->data.len = 10;

    simpleauth_id_blob = id;
}

static void simpleauth_identity_destroy(int is_task_exit)
{
    if (simpleauth_id_blob) {
        free(simpleauth_id_blob->principal);
        free(simpleauth_id_blob->data.data);
        free(simpleauth_id_blob);
        simpleauth_id_blob = NULL;
    }
}

static void *simpleauth_getIdentity(cdb2_hndl_tp *hndl, int flags)
{
    if (!simpleauth_id_blob)
        simpleauth_identity_create();
    if (!simpleauth_id_blob)
        return NULL;

    /* Return a copy — caller will manually free it */
    CDB2SQLQUERY__IdentityBlob *copy = calloc(1, sizeof(CDB2SQLQUERY__IdentityBlob));
    if (!copy)
        return NULL;

    cdb2__sqlquery__identity_blob__init(copy);

    copy->principal = strdup(simpleauth_id_blob->principal);
    if (!copy->principal) {
        free(copy);
        return NULL;
    }

    copy->majorversion = simpleauth_id_blob->majorversion;
    copy->minorversion = simpleauth_id_blob->minorversion;
    copy->data.len = simpleauth_id_blob->data.len;
    copy->data.data = malloc(simpleauth_id_blob->data.len);
    if (!copy->data.data) {
        free(copy->principal);
        free(copy);
        return NULL;
    }
    memcpy(copy->data.data, simpleauth_id_blob->data.data, simpleauth_id_blob->data.len);
    return copy;
}

static void simpleauth_resetIdentity_start(void)
{
}

static void simpleauth_resetIdentity_end(int rc)
{
}

static void simpleauth_set_identity(cdb2_hndl_tp *hndl, const void *identity)
{
}

static int simpleauth_identity_valid(void)
{
    return simpleauth_id_blob != NULL;
}

static struct cdb2_identity simpleauth_identity_callbacks = {
    .resetIdentity_start = simpleauth_resetIdentity_start,
    .resetIdentity_end = simpleauth_resetIdentity_end,
    .getIdentity = simpleauth_getIdentity,
    .set_identity = simpleauth_set_identity,
    .identity_create = simpleauth_identity_create,
    .identity_destroy = simpleauth_identity_destroy,
    .identity_valid = simpleauth_identity_valid,
};

static int simpleauth_destroy(void)
{
    return 0;
}

static int simpleauth_initialize(void)
{
    if (gbl_uses_simpleauth) {
        externalMakeNewsqlAuthData = simpleNewsqlAuthData;
        externalFreeNewsqlAuthData = simpleFreeNewsqlAuthData;
        externalComdb2CheckOpAccess = simpleCheckOPAccess;
        externalComdb2AuthenticateUserDDL = simpleCheckTableAccessDDL;
        externalComdb2AuthenticateUserRead = simpleCheckTableAccessRead;
        externalComdb2AuthenticateUserWrite = simpleCheckTableAccessWrite;
        externalComdb2AuthenticateUserMakeRequest = simpleCheckTableAccessMakeRequest;
        externalComdb2DeSerializeIdentity = simpleDeSerializeIdentity;
        externalComdb2SerializeIdentity = simpleSerializeIdentity;
        externalComdb2getAuthIdBlob = simpleGetAuthIdBlob;
        gbl_uses_externalauth = 1;
        identity_cb = &simpleauth_identity_callbacks;
    }
    return 0;
}

comdb2_initializer_t simpleauth_comdb2_plugin = {0, simpleauth_initialize};

#include "plugin.h"
