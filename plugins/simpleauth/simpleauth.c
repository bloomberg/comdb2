#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "comdb2_plugin.h"
#include "comdb2_initializer.h"
#include "sqlquery.pb-c.h"

#include "logmsg.h"

extern char gbl_dbname[];
extern int gbl_uses_simpleauth;
extern int gbl_uses_externalauth;
extern int gbl_allow_old_authn;

int cdb2_in_client_trans();

#include "mem_plugins.h"
#include "mem_override.h"

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

    /* No identity blob sent - allow access (anonymous user) */
    if (!ID)
        return 0;

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

    authenticationData *authData = (authenticationData *)ID;
    return simpleAuthCheck(authData->principal, action, resource);
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
    }
    return 0;
}

comdb2_initializer_t simpleauth_comdb2_plugin = {0, simpleauth_initialize};

#include "plugin.h"
