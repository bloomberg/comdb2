/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import com.bloomberg.comdb2.jdbc.Cdb2DbInfoResponse.*;
import com.bloomberg.comdb2.jdbc.Cdb2Query.*;
import com.bloomberg.comdb2.jdbc.Cdb2SqlResponse.*;
import com.bloomberg.comdb2.jdbc.Sqlquery.*;
import com.bloomberg.comdb2.jdbc.Sqlquery.CDB2_SQLQUERY.*;
import com.bloomberg.comdb2.jdbc.Sqlresponse.*;
import com.bloomberg.comdb2.jdbc.Sqlresponse.CDB2_DBINFORESPONSE.*;
import com.bloomberg.comdb2.jdbc.Sqlresponse.CDB2_SQLRESPONSE.*;
import com.google.protobuf.*;

public class ProtobufProtocol implements Protocol {
    private static Logger logger = Logger.getLogger(ProtobufProtocol.class.getName());

    private static final long serialVersionUID = -6216737287860982050L;

    private CDB2_QUERY _lastQuery;

    public ProtobufProtocol() {
    }

    public byte[] write(OutputStream ops) throws IOException {
        byte[] payload = null;
        if (_lastQuery != null && ops != null) {
            payload = _lastQuery.toByteArray();
            ops.write(payload);
        }
        return payload;
    }

    public Cdb2DbInfoResponse unpackDbInfoResp(byte[] bytes) throws IOException {
        try {
            Cdb2DbInfoResponse ret = new Cdb2DbInfoResponse();

            CDB2_DBINFORESPONSE _dbInfoResp = CDB2_DBINFORESPONSE
                .parseFrom(bytes);

            nodeinfo _master = _dbInfoResp.getMaster();
            ret.master = new NodeInfo(_master.getName(), _master.getNumber(),
                    _master.getIncoherent(), _master.getRoom(),
                    _master.getPort());

            if (!_dbInfoResp.hasRequireSsl())
                ret.peermode = Constants.PEER_SSL_MODE.PEER_SSL_UNSUPPORTED;
            else if (_dbInfoResp.getRequireSsl())
                ret.peermode = Constants.PEER_SSL_MODE.PEER_SSL_REQUIRE;
            else
                ret.peermode = Constants.PEER_SSL_MODE.PEER_SSL_ALLOW;

            if (_dbInfoResp.getNodesCount() > 0) {
                ret.nodes = new ArrayList<NodeInfo>(_dbInfoResp.getNodesCount());

                for (nodeinfo _node : _dbInfoResp.getNodesList())
                    ret.nodes.add(new NodeInfo(_node.getName(), _node
                                .getNumber(), _node.getIncoherent(), _node
                                .getRoom(), (_node.hasPort() ? _node.getPort() : -1)));
            }
            return ret;
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException(ipbe);
        }
    }

    public Cdb2SqlResponse unpackSqlResp(byte[] bytes) throws IOException {
        try {
            Cdb2SqlResponse ret = new Cdb2SqlResponse();
            CDB2_SQLRESPONSE _sqlresp = CDB2_SQLRESPONSE.parseFrom(bytes);

            ret.respType = _sqlresp.getResponseType().getNumber();
            ret.errCode = _sqlresp.getErrorCode().getNumber();
            ret.errStr = _sqlresp.getErrorString();

            if (_sqlresp.hasDbinforesponse()) {
                ret.dbInfoResp = new Cdb2DbInfoResponse();
                CDB2_DBINFORESPONSE _dbinfo = _sqlresp.getDbinforesponse();
                nodeinfo _master = _dbinfo.getMaster();
                ret.dbInfoResp.master = new NodeInfo(_master.getName(),
                        _master.getNumber(), _master.getIncoherent(),
                        _master.getRoom(), _master.getPort());

                if (_dbinfo.getNodesCount() > 0) {
                    ret.dbInfoResp.nodes = new ArrayList<NodeInfo>(
                            _dbinfo.getNodesCount());

                    for (nodeinfo _node : _dbinfo.getNodesList())
                        ret.dbInfoResp.nodes.add(new NodeInfo(_node.getName(),
                                _node.getNumber(), _node.getIncoherent(), _node
                                        .getRoom(), _node.getPort()));
                }
            }

            if (_sqlresp.hasEffects()) {
                CDB2_EFFECTS effects = _sqlresp.getEffects();
                ret.effects = new Effects(effects.getNumAffected(),
                        effects.getNumSelected(), effects.getNumUpdated(),
                        effects.getNumDeleted(), effects.getNumInserted());
            }

            if (_sqlresp.getValueCount() > 0) {

                ret.value = new ArrayList<Column>(_sqlresp.getValueCount());

                for (column col : _sqlresp.getValueList()) {
                    ret.value.add(
                            new Column(
                                col.hasType() ? col.getType().getNumber() : -1,
                                col.getValue().toByteArray()
                                )
                            );
                }
            }

            if (_sqlresp.hasSnapshotInfo()) {
                CDB2_SQLRESPONSE.snapshotinfo si = _sqlresp.getSnapshotInfo();
                ret.hasSnapshotInfo = true;
                ret.file = si.getFile();
                ret.offset = si.getOffset();
            }

            if (_sqlresp.hasRowId()) {
                ret.hasRowId = true;
                ret.rowId = _sqlresp.getRowId();
            }

            if (_sqlresp.getFeaturesCount() > 0) {
                ret.features = new ArrayList<Integer>();
                for (CDB2ServerFeatures f : _sqlresp.getFeaturesList())
                    ret.features.add(f.getNumber());
            } else
                ret.features = Collections.emptyList();

            return ret;
        } catch (InvalidProtocolBufferException ipbe) {
            throw new IOException(ipbe);
        }
    }

    public int pack(Cdb2Query query) {

        CDB2_QUERY.Builder _query = CDB2_QUERY.newBuilder();
        if (query.cdb2DbInfo != null) {
            Cdb2DbInfo cdb2DbInfo = query.cdb2DbInfo;
            _query.setDbinfo(CDB2_DBINFO.newBuilder()
                    .setDbname(cdb2DbInfo.dbName)
                    .setLittleEndian(cdb2DbInfo.littleEndian).build());
        }

        if (query.cdb2SqlQuery != null) {
            Cdb2SqlQuery cdb2SqlQuery = query.cdb2SqlQuery;
            CDB2_SQLQUERY.Builder _sqlquery = CDB2_SQLQUERY.newBuilder();
            _sqlquery.setDbname(cdb2SqlQuery.dbName)
                    .setSqlQuery(cdb2SqlQuery.sqlQuery)
                    .setLittleEndian(cdb2SqlQuery.littleEndian)
                    .setTzname(cdb2SqlQuery.tzName)
                    .setMachClass(cdb2SqlQuery.machClass);

            if (cdb2SqlQuery.cinfo != null)
                _sqlquery.setClientInfo(CDB2_SQLQUERY.cinfo.newBuilder().
                        setPid(cdb2SqlQuery.cinfo.pid).setThId(cdb2SqlQuery.cinfo.tid).
                        setHostId(cdb2SqlQuery.cinfo.host_id).setArgv0(cdb2SqlQuery.cinfo.argv0).
                        setStack(cdb2SqlQuery.cinfo.stack));

            if (cdb2SqlQuery.cnonce != null)
                _sqlquery.setCnonce(ByteString.copyFrom(cdb2SqlQuery.cnonce));

            for (Cdb2Flag flag : cdb2SqlQuery.flag)
                _sqlquery.addFlag(CDB2_FLAG.newBuilder().setOption(flag.option)
                        .setValue(flag.value).build());

            for (Cdb2BindValue bindVar : cdb2SqlQuery.bindVars)
                _sqlquery.addBindvars(bindvalue.newBuilder()
                        .setVarname(bindVar.varName).setType(bindVar.type)
                        .setValue(ByteString.copyFrom(bindVar.value == null ? new byte[]{} : bindVar.value)).build());

            for (String setFlag : cdb2SqlQuery.setFlags)
                _sqlquery.addSetFlags(setFlag);

            for (int type : cdb2SqlQuery.types)
                _sqlquery.addTypes(type);

            if (cdb2SqlQuery.hasSnapshotInfo)
                _sqlquery.setSnapshotInfo(CDB2_SQLQUERY.snapshotinfo.newBuilder().
                        setFile(cdb2SqlQuery.file).setOffset(cdb2SqlQuery.offset));

            if (cdb2SqlQuery.hasSkipRows)
                _sqlquery.setSkipRows(cdb2SqlQuery.skipRows);

            if (cdb2SqlQuery.hasRetry)
                _sqlquery.setRetry(cdb2SqlQuery.retry);

            if (cdb2SqlQuery.features.size() > 0)
                _sqlquery.addAllFeatures(cdb2SqlQuery.features);

            if (cdb2SqlQuery.reqInfo != null)
                _sqlquery.setReqInfo(CDB2_SQLQUERY.reqinfo.newBuilder().setTimestampus(cdb2SqlQuery.reqInfo.timestampus).setNumRetries(cdb2SqlQuery.reqInfo.numretries));

            _query.setSqlquery(_sqlquery.build());
        }

        _lastQuery = _query.build();
        return _lastQuery.getSerializedSize();
    }
}
/* vim: set sw=4 ts=4 et: */
