package com.ea.asiacentraltech.thoth.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.ea.asiacentraltech.thoth.common.Constant;
import com.ea.asiacentraltech.thoth.common.MyOwnRuntimeException;
import com.ea.asiacentraltech.thoth.common.Utils;
import com.ea.asiacentraltech.thoth.job.ConstantJob;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

/**
 * mongoDB Class mongoDB 관련 명령을 담당
 *
 */
public class MongoDB {
  private static final Pattern NUMERIC = Pattern.compile("[-+]?\\d*\\.?\\d+");
	
  private List<ServerAddress> connList = new ArrayList<>();
  private MongoClient mongoClient;
  private MongoDatabase db;
  private Logger logger = null;
  private MongoCredential mongoCredential;
  private boolean statusSharding = true;
  private Map<String, String> shardingMap = new HashMap<>();
  private int shardingError = 0;
  private int initialChunksSize = ConstantMongodb.MONGODB_INITIAL_CHUNK_SIZE;
  private String nodeID = new ObjectId().toString();
  private Utils utils = new Utils();

  /**
   * mongo DB Connect
   * 
   * @throws Exception
   */
  private void connect() {
    if (mongoClient != null)
      mongoClient.close();

    if (mongoCredential != null) {
      mongoClient = new MongoClient(connList, Arrays.asList(mongoCredential));
    } else {
      mongoClient = new MongoClient(connList);
    }
    db = this.getDatabase(ConstantMongodb.DATABASE_ADMIN);
    this.setDefaultParameter();
    this.checkSharding();
  }

  /**
   * MongoDB에 대해서 Parameter 설정을 한다.
   * 
   * @param millis
   */
  private void setDefaultParameter() {
    Document parameter = new Document();
    parameter.put("setParameter", 1);
    parameter.put("cursorTimeoutMillis", 60000);
    // parameter.put("notablescan", 1); // 반드시 index Scan을 할거라는 보장을 할 수 가 없다.

    this.runCommand(ConstantMongodb.DATABASE_ADMIN, parameter);
  }

  /**
   * database connect
   * 
   * @param String
   *          host
   * @param String
   *          port
   * @return void
   */
  public void conn(final String host, final int port) {
    connList = new ArrayList<>();
    connList.add(new ServerAddress(host, port));

    this.connect();
  }

  public void setChunkSize(final int num) {
    if (num < 1) return;
    this.initialChunksSize = num;
  }
  /**
   * bind Logger
   * 
   * @param logger
   */
  public void bindLogger(Logger logger) {
    this.logger = logger;
  }

  /**
   * database Multi connect
   * 
   * @param String
   *          host
   * @return void
   * @throws Exception
   */
  public void multiConn(final String hostAddress) {
    if (hostAddress == null)
      throw new NullPointerException("mongoDB Server");
    connList = new ArrayList<>();
    String[] hosts = hostAddress.replaceAll("mongodb://", "").split(",");

    String hostName;
    int hostPort;

    for (String host : hosts) {
      String[] hostInfo = host.split(":");
      if (hostInfo.length != 2)
        continue;
      hostName = hostInfo[0];
      try {
        hostPort = Integer.parseInt(hostInfo[1]);
        connList.add(new ServerAddress(hostName, hostPort));
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    if (connList.isEmpty())
      throw new NullPointerException("mongoDB Server");
    java.util.Collections.shuffle(connList);

    this.connect();
  }

  /**
   * change Database
   * 
   * @param String
   *          database
   * @return void
   * @throws Exception
   */
  public void setDB(final String database) {
    this.keepAlive();

    if (this.getDB().equals(database))
      return;
    this.db = this.getDatabase(database);
  }

  /**
   * database authenticate
   * 
   * @param String
   *          id
   * @param String
   *          password
   * @return void
   */

  public void auth(final String id, final String password) {
    if (id == null || password == null)
      return;
    mongoCredential = MongoCredential.createScramSha1Credential(id, ConstantMongodb.DATABASE_ADMIN,
        password.toCharArray());
  }

  /**
   * Document Insert
   * 
   * @param String
   *          collectionName
   * @param BasicDBObject
   *          Document
   * @return void
   * @throws Exception
   */
  public int docInsert(final String collName, final Document doc) {
    int rtn = 0;
    MongoCollection<Document> coll = this.getCollectionPrimary(collName).withWriteConcern(WriteConcern.ACKNOWLEDGED);
    try {
      coll.insertOne(doc);
    } catch (MongoException ex) {
      rtn = ex.getCode();
    } catch (IllegalArgumentException e) {
      try {
        coll.insertOne(replaceIllegalArgumentException(doc));
      } catch (MongoException ex) {
        rtn = ex.getCode();
      }
    }
    return rtn;
  }

  /**
   * replaceIllegalArgumentException이 발생되는 Key 이름에 . 이 붙는 경우는 _로 치환한다.
   * 
   * @param doc
   * @return
   */
  private Document replaceIllegalArgumentException(Document doc) {
    Document chgDoc = new Document();
    String chgKey = "";

    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();

      chgKey = key;
      if (value instanceof Document) {
        doc.put(key, replaceIllegalArgumentException((Document) value));
      } else if (value instanceof List) {
        @SuppressWarnings("unchecked")
        List<Document> docList = (List<Document>) doc.get(key);
        for (int i = 0; i < docList.size(); i++) {
          if (docList.get(i) instanceof Document) {
            docList.set(i, replaceIllegalArgumentException((Document) docList.get(i)));
          }
        }
      }

      if (key.contains("."))
        chgKey = key.replaceAll("\\.", "_");
      if (key.contains("$"))
        chgKey = key.replaceAll("\\$", "_");
      chgDoc.append(chgKey, doc.get(key));
    }

    return chgDoc;
  }

  /**
   * Database의 Collection을 반환한다.
   * 
   * @param dbName
   * @param collName
   * @return
   */
  public MongoCollection<Document> getCollection(final String collName) {
    return this.getCollection(db, collName, ReadPreference.secondaryPreferred());
  }

  public MongoCollection<Document> getCollectionPrimary(final String collName) {
    return this.getCollection(db, collName, ReadPreference.primary());
  }

  /**
   * Database의 Collection을 반환한다.
   * 
   * @param dbName
   * @param collName
   * @return
   */
  public MongoCollection<Document> getCollection(final String dbName, final String collName) {
    return this.getCollection(this.getDatabase(dbName), collName, ReadPreference.secondaryPreferred());
  }

  public MongoCollection<Document> getCollectionPrimary(final String dbName, final String collName) {
    return this.getCollection(this.getDatabase(dbName), collName, ReadPreference.primary());
  }

  private MongoCollection<Document> getCollection(final MongoDatabase database, final String collName,
      final ReadPreference mode) {
    return database.getCollection(collName).withReadPreference(mode);
  }

  public MongoDatabase getDatabase(final String dbName) {
    return mongoClient.getDatabase(dbName);
  }

  public Set<String> getDatabaseNames() {
    HashSet<String> databaseNames = new HashSet<>();
    MongoIterable<String> databases = mongoClient.listDatabaseNames();
    for (String databaseName : databases) {
      databaseNames.add(databaseName);
    }
    return databaseNames;
  }
  /**
   * Document FindOne
   * 
   * @param collName
   * @param query
   * @return
   */
  public Document docFindOne(final String collName, final Document query) {
    MongoCollection<Document> coll = db.getCollection(collName);

    return coll.find(query).first();
  }

  /**
   * Document FindOne
   * 
   * @param dbName
   * @param collName
   * @param query
   * @return
   */
  public Document docFindOne(final String dbName, final String collName, final Document query) {
    return this.getCollection(dbName, collName).find(query).first();
  }

  /**
   * Document FindOne
   * 
   * @param dbName
   * @param collName
   * @param query
   * @param fields
   * @return
   */
  public Document docFindOne(final String dbName, final String collName, final Document query, final Document fields) {
    return this.getCollection(dbName, collName).find(query).projection(fields).first();
  }

  /**
   * document Update
   * 
   * @param collName
   * @param query
   * @param doc
   * @param multi
   * @throws Exception
   */
  public ResultMongoDB docUpdate(final String collName, final Document query, final Document doc, final Boolean upsert,
      final Boolean multi) {
    return this.docUpdate(this.getCollectionPrimary(collName), query, doc, upsert, multi);
  }
  
  /**
   * document Update
   * 
   * @param dbName
   * @param collName
   * @param query
   * @param doc
   * @param upsert
   * @param multi
   * @return
   */
  public ResultMongoDB docUpdate(final String dbName, final String collName, final Document query, final Document doc, final Boolean upsert,
      final Boolean multi) {
    return this.docUpdate(this.getCollectionPrimary(dbName, collName), query, doc, upsert, multi);
  }
  
  /**
   * document Update
   * 
   * @param collName
   * @param query
   * @param doc
   * @param multi
   * @throws Exception
   */
  private ResultMongoDB docUpdate(final MongoCollection<Document> collection, final Document query, final Document doc, final Boolean upsert,
      final Boolean multi) {
    ResultMongoDB rtnResult = new ResultMongoDB();
    MongoCollection<Document> coll = collection.withWriteConcern(WriteConcern.ACKNOWLEDGED);
    UpdateOptions options = new UpdateOptions().upsert(upsert);
    try {
      UpdateResult wResult;
      if (multi) {
        wResult = coll.updateMany(query, doc, options);
      } else {
        wResult = coll.updateOne(query, doc, options);
      }
      rtnResult.setMatchN(wResult.getMatchedCount());
      rtnResult.setResultN(wResult.getModifiedCount());
    } catch (MongoException ex) {
      logger.log(Level.SEVERE, "MongoException {0}\nCollName: {3}\nQuery : {1}\nDoc : {2}",
          new Object[] { ex.getMessage(), query.toString(), doc.toString(), collection.getNamespace().getFullName() });
      rtnResult.setException(true);
      rtnResult.setExCode(ex.getCode());
      rtnResult.setExMessage(ex.getMessage());
      return rtnResult;
    }

    return rtnResult;
  }

  /**
   * Return Current Database Name
   * 
   * @return String
   */
  public String getDB() {
    return db.getName();
  }

  /**
   * run Command
   * 
   * @param database
   * @param cmd
   * @return
   */
  public Document runCommand(final String database, final Document cmd) {
    MongoDatabase rdb = this.getDatabase(database);
    return this.runCommand(rdb, cmd);
  }
  
  public boolean isSharding(final String dbName, final String collName) {
    return this.shardingMap.containsKey(collName) && shardingMap.get(collName).equals(dbName);
  }

  private Document runCommand(MongoDatabase db, final Document cmd) {
    Document result;
    try {
      result = db.runCommand(cmd);
    } catch (com.mongodb.MongoCommandException e) {
      result = new Document("ok", (double) -1);
      result.append("code", e.getErrorCode());
      result.append("message", e.getErrorMessage());
    }
    result.append(ConstantMongodb.COMMAND_OK, result.getDouble("ok") == 1 );
    return result;
  }
  
  /**
   * Sharding 상태를 확인한다. 다수의 Node간에 충돌을 방지한다.
   * 상태가 STATUS_COLLECTION_NOT_EXISTS 일 경우 STATUS_COLLECTION_PROGRESS로 바꾸고  true를 반환한다.
   * 이미 진행중이라면 샤딩이 끝날때까지 최대 1분간 대기한다.
   * 진행이 완료되지 않은 경우(!DONE) 샤딩을 한번만 더 시도한다.
   * @param dbName
   * @param collName
   * @return
   */
  private boolean checkCollection(final String dbName, final String collName) {
    int collStatus = this.getCollectionStatus(dbName, collName);
    if (collStatus == ConstantMongodb.STATUS_COLLECTION_NOT_EXISTS) {
      if (!this.setCollectionStatus(dbName, collName)) {
        return checkCollection(dbName, collName);
      }
      return true;
    } else if (collStatus == ConstantMongodb.STATUS_COLLECTION_PROGRESS) {
      this.waitShardingTransaction(dbName, collName, collStatus);
    }
    this.shardingMap.put(collName, dbName);
    return this.getCollectionStatus(dbName, collName) != ConstantMongodb.STATUS_COLLECTION_DONE;
  }
  /**
   * Sharding 처리상태가 STATUS_COLLECTION_PROGRESS 일때,
   * 일정 시간동안 대기함으로써 Sharding 처리 중에 Insert Opertaion이 충돌되지 않도록 한다.
   * 최대 1분을 대기하며, 1분이 초과될 경우 무시하고 진행한다. 
   * @param dbName
   * @param collName
   * @param status
   */
  private void waitShardingTransaction(final String dbName, final String collName, int status) {
    long startTs = new Date().getTime();
    while(true) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (this.getCollectionStatus(dbName, collName) == ConstantMongodb.STATUS_COLLECTION_DONE) return;
      if (new Date().getTime() - startTs > ConstantMongodb.MONGODB_SHARDING_WAIT_TIME) {
        this.ignoreCollectionStatus(dbName, collName);
        return;
      }
    }
  }
  /**
   * Collection Sharding 상태를 반환한다.
   * @param dbName
   * @param collName
   * @return int ( -1 : not sharding, 0: Lock(ing), 1: Done, 2 : Fail ) 
   */
  private int getCollectionStatus(final String dbName, final String collName) {
    Document query = new Document(ConstantMongodb.KEY_META_LOGDBS_DATABASE, dbName);
    query.append(ConstantMongodb.KEY_META_LOGDBS_COLLECTION, collName);
    Document doc = this.getCollection(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_LOGDBS).find(query).first();
    if (doc == null) return ConstantMongodb.STATUS_COLLECTION_NOT_EXISTS;
    return (int) utils.docGetDouble(doc, ConstantMongodb.KEY_META_LOGDBS_STATUS, ConstantMongodb.STATUS_COLLECTION_FAILED);
  }

  private boolean setCollectionStatus(final String dbName, final String collName) {
    Document doc = new Document();
    String id = this.getShardingID(dbName, collName);
    doc.append(ConstantMongodb.ID, id);
    doc.append(ConstantMongodb.KEY_META_LOGDBS_DATABASE, dbName);
    doc.append(ConstantMongodb.KEY_META_LOGDBS_COLLECTION, collName);
    doc.append(ConstantMongodb.KEY_META_LOGDBS_STATUS, ConstantMongodb.STATUS_COLLECTION_PROGRESS);
    doc.append(ConstantMongodb.KEY_META_LOGDBS_NODE, nodeID);
    doc.append("ts", new Date().getTime());
    try {
      this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_LOGDBS).insertOne(doc);
      return true;
    } catch (MongoException e) {
      if (shardingError ++ > 3) throw new MyOwnRuntimeException("sharding setCollectionStatus Fail", id);
    }
    try {
      Thread.sleep(500);
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
    return false;
  }
  
  private void ignoreCollectionStatus(final String dbName, final String collName) {
    Document query = new Document(ConstantMongodb.ID, this.getShardingID(dbName, collName));
    Document fields = new Document(ConstantMongodb.KEY_META_LOGDBS_STATUS, ConstantMongodb.STATUS_COLLECTION_FAILED);
    this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_LOGDBS).updateOne(query, new Document("$set", fields));
  }
  
  /**
   * Database Sharding 이 완료되었을때 후처리
   * 
   * @param database
   */
  private void afterSharding(final String dbName, final String collName, final boolean ok) {
    Document query = new Document(ConstantMongodb.ID, this.getShardingID(dbName, collName));
    query.append(ConstantMongodb.KEY_META_LOGDBS_NODE, nodeID);
    Document fields = new Document(ConstantMongodb.KEY_META_LOGDBS_STATUS, ok ? ConstantMongodb.STATUS_COLLECTION_DONE : ConstantMongodb.STATUS_COLLECTION_NOT_EXISTS );
    this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_LOGDBS).updateOne(query, new Document("$set", fields));
  }

  /**
   * Init File Path No
   * 
   * @return String
   * @throws Exception
   */
  public void initMetaFilesystem() {
    MongoCollection<Document> coll = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_FILESYSTEM);
    Document setDocument = new Document("$set", new Document().append("no", 0));
    coll.updateMany(new Document(), setDocument);
  }

  /**
   * Remove Database Info
   * 
   * @return String
   * @throws Exception
   */
  public void dropDatabase(final String database) {
    MongoCollection<Document> logdbsColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_LOGDBS);
    logdbsColl.deleteOne(new Document().append(ConstantMongodb.KEY_META_LOGDBS_DATABASE, database));
    this.getDatabase(database).drop();
  }

  /**
   * Drop Collection
   * 
   * @param collName
   * @throws Exception
   */
  public void dropCollection(final String database, final String collName) {
    this.getCollectionPrimary(database, collName).drop();
  }

  /**
   * new Database enable Sharding
   * 
   * @param String
   *          database
   * @return void
   * @throws Exception
   */
  private void enableSharding(final String database) {
    if (ConstantMongodb.DATABASE_ADMIN.equals(database)
        || ConstantMongodb.DATABASE_CONFIG.equals(database)
        || !statusSharding)
      return;
  
    Document cmd = new Document("enableSharding", database);
    Document databaseResult = this.runCommand(ConstantMongodb.DATABASE_ADMIN, cmd);

    if (!databaseResult.getBoolean(ConstantMongodb.COMMAND_OK, false)) {
      switch (databaseResult.getInteger(ConstantMongodb.COMMAND_CODE, 0)) {
        case -5:
        case ConstantMongodb.ERROR_DATABASE_SHARDING_ALREADY_ENABLED: // "sharding already enabled
          break;
        default:
          logger.log(Level.WARNING, "[Exception] enableSharding Fail : {0} \n{1}",
              new Object[] { database, databaseResult });
          break;
      }
    }
  }

  /**
   * Keep Alive
   * 
   * @param mongo
   * @throws Exception
   */
  public void keepAlive() {
    if (!this.runCommand("admin", new Document("ping", 1)).getBoolean(ConstantMongodb.COMMAND_OK, false)) {
      this.connect();
      if (mongoClient.getAddress() == null)
        throw new MyOwnRuntimeException("keepAlive Fail");
    }
  }

  /**
   * Collection Sharding
   * @param collName
   */
  public void collectionSharding(final String collName) {
    String dbName = db.getName();
    if (ConstantMongodb.DATABASE_META.equals(dbName) || ConstantMongodb.DATABASE_ADMIN.equals(dbName))
      return;
    if (this.isSharding(dbName, collName)) return;
    MongoCollection<Document> coll = db.getCollection(collName);
    this.enableSharding(dbName);
    if (this.checkCollection(dbName, collName)) {
      this.waitGlobalLock(ConstantJob.MONGODB_SHARDING, ConstantMongodb.MONGODB_SHARDING_LOCK_IGNORE_SEC);
      this.ensureSharding(dbName, collName);
      this.ensureIndex(collName, coll);
      this.afterSharding(dbName, collName, true);
      this.terminateGlobalLock(ConstantJob.MONGODB_SHARDING);
      this.shardingError = 0;
    }
  }

  private String getShardingID(final String dbName, final String collName) {
    return dbName + collName;
  }
  /**
   * Collection Hash Sharding
   * @param dbName
   * @param collName
   * @param threadName
   */
  private void ensureSharding(final String dbName, final String collName) {
    if (!statusSharding) {
      return;
    }
    Document cmd = new Document();
    cmd.append("shardCollection", new StringBuilder(dbName).append(".").append(collName).toString());
    cmd.append("key", new Document(ConstantMongodb.ID, "hashed")).append("numInitialChunks", this.initialChunksSize);
    int reTryCount = 0;
    while (true) {
      Document databaseResult = this.runCommand(ConstantMongodb.DATABASE_ADMIN, cmd);
      if (!databaseResult.getBoolean(ConstantMongodb.COMMAND_OK, false)) {
        int exceptionCode = databaseResult.getInteger(ConstantMongodb.COMMAND_CODE, 0);
        if (exceptionCode == ConstantMongodb.ERROR_COLLECTION_ALREADY_SHARED
            || exceptionCode == ConstantMongodb.ERROR_DB_DOSE_NOT_HAVE_SHARDING_ENABLED
            || exceptionCode == ConstantMongodb.ERROR_COLLECTION_SHARDING_ALREADY_ENABLED) {
          return;
        } else {
          logger.log(Level.WARNING, "[Exception] shardCollection Fail-{3} : {0}.{1} [{2}] \n {4}",
              new Object[] { dbName, collName, exceptionCode, reTryCount, databaseResult.toString() });
        }
      } else {
        return;
      }
      if (++reTryCount > 10) return;
    }
  }

  /**
   * ensureIndex
   * @param collName
   * @param coll
   */
  private void ensureIndex(final String collName, MongoCollection<Document> coll) {
    MongoCursor<Document> cursor = this.getCollection(ConstantMongodb.DATABASE_META,
                                                      ConstantMongodb.COLLECTION_META_INDEXES)
                                       .find(new Document(ConstantMongodb.KEY_META_LOGDBS_COLLECTION, collName))
                                       .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                                       .iterator();
    List<Document> docList = new ArrayList<>();
    while (cursor.hasNext()) {
      docList.add(cursor.next());
    }
    if (docList.isEmpty()) return;
    String indexName = null;
    for (Document doc : docList) {
      IndexOptions idxOption = new IndexOptions();
      indexName = doc.getString(ConstantMongodb.ID);
      idxOption.name(indexName);
      Object keys = doc.get(ColletionInfo.STR_KEYS);
      if (keys == null || ((Document) keys).isEmpty()) continue;
      Object option = doc.get(ColletionInfo.STR_EXPRESSION);
      if (option != null && !(option.toString().isEmpty())) {
        try {
          idxOption.partialFilterExpression(Document.parse(option.toString()));
        } catch (com.mongodb.util.JSONParseException | org.bson.json.JsonParseException e){
          this.logger.log(Level.WARNING, "Index Option Parsing Failed {0}", new Object[] { indexName });
          continue;
        }
      }
      this.createIndex(coll, (Document) keys, idxOption);
    }
  }

  /**
   * Create User
   * 
   * @param userName
   * @param password
   * @throws Exception
   */
  public void createUser(final String databaseName, final String userName, final String password) {
    if (userName == null || password == null || userName.isEmpty() || password.isEmpty())
      return;

    if (ConstantMongodb.DATABASE_META.equals(databaseName) || ConstantMongodb.DATABASE_ADMIN.equals(databaseName))
      return;

    Document commandArguments = new Document();
    commandArguments.put("createUser", userName);
    commandArguments.put("pwd", password);
    String[] roles = { "read" };
    commandArguments.put("roles", roles);
    Document command = new Document(commandArguments);
    Document databaseResult = this.runCommand(databaseName, command);

    if (!databaseResult.getBoolean(ConstantMongodb.COMMAND_OK, false)
        && databaseResult.getInteger(ConstantMongodb.COMMAND_CODE, 0) != ConstantMongodb.ERROR_DUPLICATE_KEY) {
      // Exception, already exists은 제외한다.
      logger.log(Level.WARNING, "[Exception] Create User Fail : {0}.{1}\n{2}",
          new Object[] { databaseName, userName, databaseResult.getInteger(ConstantMongodb.COMMAND_CODE, 0) });
    }
  }

  /**
   * get CollctionList
   * 
   * 
   * @return Set<String>
   */
  public Set<String> getCollctions() {
    return this.listCollectionNames(db);
  }

  /**
   * get Collection List
   * 
   * @param dbName
   * @return
   */
  public Set<String> getCollcetions(final String dbName) {
    return this.listCollectionNames(this.getDatabase(dbName));
  }
  private Set<String> listCollectionNames(MongoDatabase database) {
    Set<String> result = new HashSet<>();
    MongoIterable<String> collections = database.listCollectionNames();
    for (String collectionName : collections) {
      result.add(collectionName);
    }
    return result;
  }

  /**
   * mongoDB db.stats()
   * 
   * @return
   * @see { "raw" : { "Log_S1/10.88.18.20:42001,10.88.18.20:42002" : { "db" :
   *      "admin", "collections" : 0, "objects" : 0, "avgObjSize" : 0,
   *      "dataSize" : 0, "storageSize" : 0, "numExtents" : 0, "indexes" : 0,
   *      "indexSize" : 0, "fileSize" : 0, "ok" : 1, "$gleStats" : {
   *      "lastOpTime" : Timestamp(0, 0), "electionId" :
   *      ObjectId("568b232d7b2db4bec14d13ee") } } }, "objects" : 0,
   *      "avgObjSize" : 0, "dataSize" : 0, "storageSize" : 0, "numExtents" : 0,
   *      "indexes" : 0, "indexSize" : 0, "fileSize" : 0, "extentFreeList" : {
   *      "num" : 0, "totalSize" : 0 },
   * 
   */
  public Document stats() {
    return this.runCommand(db, new Document("dbStats", 1));
  }

  /**
   * 등록된 FileSystem이 있는지 확인한다.
   * 
   * @return
   * @throws Exception
   */
  public boolean existsMetaFileSystem() {
    MongoCollection<Document> coll = this.getCollection(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_FILESYSTEM);
    return coll.count() > 0;
  }



  /**
   * set Meta FileInfo
   * 
   * @param String
   *          path
   * @param String
   *          fileName
   * @param String
   *          dbName
   * @param String
   *          collName
   * @param String
   *          type
   * @param long
   *          dataCount
   * @return void
   * @throws Exception
   */
  public void setMetaFileInfo(final String path, final String fileName, final String dbName, final String collName,
      final String type, final long dataCount) {

    this.setDB(ConstantMongodb.DATABASE_META);
    Document dataInfo = new Document("database", dbName);
    dataInfo.append("collection", collName).append("type", type).append("datacount", dataCount);
    Document fileInfo = new Document("path", path);
    fileInfo.append("filename", fileName).append("fileinfo", dataInfo);
    this.docInsert(ConstantMongodb.COLLECTION_META_FILEINFO, fileInfo);
  }

  /**
   * Sharding 환경 확인 stats() 결과에 raw값 존재 여부로 확인한다.
   */
  private void checkSharding() {
    Document result = this.stats();
    statusSharding = false;

    if (result != null && result.get("raw") != null)
      statusSharding = true;
    logger.log(Level.INFO, "Set Sharding option {0}", statusSharding);
  }



  /**
   * collection을 Distinct 한 값을 반환한다.
   * 
   * @param collname
   * @param key
   * @return
   */
  public List<String> distinct(final String collname, final String key) {
    return this.distinct(this.getCollection(collname), key);
  }

  /**
   * collection을 Distinct 한 값을 반환한다.
   * 
   * @param dbName
   * @param collname
   * @param key
   * @return
   */
  public List<String> distinct(final String dbName, final String collname, final String key) {
    return this.distinct(this.getCollection(dbName, collname), key);
  }

  private List<String> distinct(MongoCollection<Document> coll, final String key) {
    List<String> rtn = new ArrayList<>();

    for (String doc : coll.distinct(key, String.class)) {
      rtn.add(doc);
    }

    return rtn;
  }

  /**
   * config에 설정된 기능을 켜고 끈다.
   * 
   * @param key
   * @param stopped
   */
  public void configSetting(final String key, final boolean stopped) {
    MongoCollection<Document> coll = this.getCollection(ConstantMongodb.DATABASE_CONFIG,
        ConstantMongodb.COLLECTION_CONFIG_SETTING);
    coll.updateOne(new Document(ConstantMongodb.ID, key), new Document("$set", new Document("stopped", stopped)));
  }

  /**
   * 기본 Meta Index를 생성한다.
   */
  public void createDefMetaIndex() {
    MongoCollection<Document> targetColl = null;

    // filesystems
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_FILESYSTEM);
    this.createIndex(targetColl, new Document("no", 1));

    // fileinfos
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_FILEINFO);
    this.createIndex(targetColl, new Document("fileinfo.database", 1));
    this.createIndex(targetColl, new Document("fileinfo.collection", 1));

    // fileindexes
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_FILEINDEXES);
    this.createIndex(targetColl, new Document("coll", 1));

    // filesystems
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_FILESYSTEM);
    this.createIndex(targetColl, new Document("no", 1));
    this.createIndex(targetColl, new Document("path", 1));

    // Accounts
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_ACCOUNTS);
    targetColl.createIndex(new Document("id", 1), new IndexOptions().unique(true).name("uniqueAccountID"));

    // systeminfo
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_ANALYSIS,
        ConstantMongodb.COLLECTION_ANALYSIS_SYSTEMSTATUS);
    targetColl.createIndex(new Document("name", 1));
    
    // COLLECTION_META_LOGDBS
    targetColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_LOGDBS);
    targetColl.createIndex(new Document(ConstantMongodb.KEY_META_LOGDBS_DATABASE, 1).append(ConstantMongodb.KEY_META_LOGDBS_COLLECTION, 1));
  }

  /**
   * MetaFilesystemCollectionName Path를 추가한다.
   * 
   * @param path
   */
  public void addClusterFilePath(final String path) {
    MongoCollection<Document> cFilePathColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_FILESYSTEM);
    Document doc = new Document("path", path);

    if (cFilePathColl.count(doc) == 0) {
      doc.append("no", 0);
      cFilePathColl.insertOne(doc);
    }
  }

  /**
   * MetaFilesystemCollectionName Path를 제거한다.
   * 
   * @param path
   */
  public void removeClusterFilePath(final String id) {
    MongoCollection<Document> cFilePathColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_FILESYSTEM);
    try {
      cFilePathColl.deleteOne(new Document(ConstantMongodb.ID, new ObjectId(id)));
    } catch (java.lang.IllegalArgumentException e) {
      return;
    }
  }

  /**
   * Chart를 생성한다.
   * 
   * @param path
   */
  public void makeChart(final String name, final String coll, final int depth, final boolean daily5mininc) {
    MongoCollection<Document> cChartInfo = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_CHARTLIST);
    Document doc = new Document(ConstantMongodb.ID, name);
    doc.append("coll", coll).append("depth", depth);
    doc.append("inc5min", daily5mininc);

    cChartInfo.insertOne(doc);
  }

  /**
   * Chart를 제거한다.
   * 
   * @param name
   */
  public void removeChart(final String name) {
    MongoCollection<Document> cChartInfo = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_CHARTLIST);
    cChartInfo.deleteOne(new Document(ConstantMongodb.ID, name));
  }

  /**
   * Chart List를 반환한다.
   * 
   * @return
   */
  public Set<String> getChartList() {
    MongoCollection<Document> cChartInfo = this.getCollection(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_CHARTLIST);
    Set<String> chartResult = new HashSet<>();
    for (Document doc : cChartInfo.find(new Document()).projection(new Document(ConstantMongodb.ID, 1)))
      chartResult.add(doc.get(ConstantMongodb.ID).toString());
    return chartResult;
  }

  /**
   * DBObject의 Depth value를 반환한다.
   * 
   * @param doc
   * @param depth
   */
  public Map<String, Integer> getDepthValue(Document doc, int depth) {
    Set<String> keyList = getKeyList(doc);
    Map<String, Integer> result = new HashMap<>();
    for (String key : keyList) {
      if (key.split("\\.").length != depth)
        continue;
      result.put(key, getDocValue(doc, key));
    }
    return result;
  }

  /**
   * .으로 구분된 key의 int value를 반환한다.
   */
  public int getDocValue(Document obj, final String keyInfo) {
    String[] keys = keyInfo.split("\\.");

    Object doc = obj;
    for (String key : keys) {
      if (doc instanceof Document)
        doc = ((Document) doc).get(key);
    }

    try {
      return Integer.parseInt(doc.toString());
    } catch (NumberFormatException | NullPointerException e) {
      return 0;
    }
  }

  /**
   * get Key List
   * 
   * @param doc
   * @return
   */
  public Set<String> getKeyList(Document doc) {
    Set<String> keyList = new HashSet<>();

    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      if (value instanceof Document) {
        for (String keyName : getKeyList((Document) value)) {
          keyList.add(new StringBuilder(key).append(".").append(keyName).toString());
        }
      } else {
        keyList.add(key);
      }
    }

    return keyList;
  }

  /**
   * isNumeric
   * 
   * @param str
   * @return
   */
  public static boolean isNumeric(String str) {
    return str != null && NUMERIC.matcher(str).matches();
  }

  /**
   * Collection의 Index를 생성한다.
   * 
   * @param coll
   * @param doc
   */
  private void createIndex(MongoCollection<Document> coll, Document doc) {
    try {
      coll.createIndex(doc);
    } catch (Exception e) {
      logger.log(Level.FINE, "Exception createIndex {0}", e);
    }
  }

  /**
   * Collection의 Index를 생성한다.
   * 
   * @param coll
   * @param doc
   */
  private void createIndex(MongoCollection<Document> coll, Document keys, IndexOptions opts) {
    try {
      coll.createIndex(keys, opts);
    } catch (com.mongodb.MongoCommandException e) {
      logger.log(Level.FINE, "Exception createIndex {0}", new Object[] {e});
    }
  }
  /**
   * jobId가 GloablLock에서 해소 될때까지 대기한다.
   * @param jobId
   * @param ignoreLockTime
   */
  private void waitGlobalLock(final String jobId, final int ignoreLockTime) {
    while(true) {
      if (this.startGlobalLock(jobId, new ObjectId(), ignoreLockTime)) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
  /**
   * 전체에서 jobID에 해당하는 Job이 동시에 한번만 수행하기 위해 GlbalLock을 시작한다.
   * @param jobID
   * @param tID
   * @param waitingSeconds
   * @return
   */
  public boolean startGlobalLock(final String jobID, final ObjectId tID, final int waitingSeconds) {
    MongoCollection<Document> cDailyJobColl = this
        .getCollectionPrimary(ConstantMongodb.DATABASE_META, ConstantMongodb.COLLECTION_META_DAILYJOB)
        .withWriteConcern(WriteConcern.ACKNOWLEDGED);
    Date checkDate = new Date();
    Document query = new Document(ConstantMongodb.ID, jobID);
    Document result = cDailyJobColl.find(query).first();
    if (result == null) {
      Document doc = new Document(ConstantMongodb.ID, jobID);
      doc.append("tID", tID).append("_t", getZeroOid());
      try {
        cDailyJobColl.insertOne(doc);
      } catch (Exception e) {
        return false;
      }
      result = doc;
    }
    ObjectId resultID = result.getObjectId("_t");
    if (resultID == null) resultID = getZeroOid();
    if (checkDate.getTime() - resultID.getDate().getTime() < (1000 * waitingSeconds)) 
      return false;
    query.put("_t", resultID);
    Document update = new Document("$set", new Document("_t", tID));
    Document doc = cDailyJobColl.findOneAndUpdate(query, update,
        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER).upsert(false).projection(new Document("_t", 1)));
    return doc.getObjectId("_t").toString().equals(tID.toString());
  }

  /**
   * GlobalLock을 해제한다.
   * @param jobID
   */
  public void terminateGlobalLock(final String jobID) {
    MongoCollection<Document> cDailyJobColl = this.getCollectionPrimary(ConstantMongodb.DATABASE_META,
        ConstantMongodb.COLLECTION_META_DAILYJOB);
    cDailyJobColl.updateOne(new Document(ConstantMongodb.ID, jobID), new Document("$set", new Document("_t", getZeroOid())));
  }

  private ObjectId getZeroOid() {
    return new ObjectId("000000000000000000000000");
  }
  
  public Document collStats(String dbName, String collName) {
    return this.runCommand(this.getDatabase(dbName), new Document("collStats", collName).append("scale",1024));
  }
  /**
   * close
   */
  public void close() {
    if (mongoClient != null)
      mongoClient.close();
  }
  public Logger getLogger() {
    return this.logger;
  }
  public long getShardCount() {
    return this.getCollectionPrimary("config", "shards").count(new Document("state", 1));
  }
  /**
   * Analysis Collection의 Summary를 반환한다.
   * @param collName
   * @param date
   * @return
   */
  public Document getAnalysisSummary(final String collName, final String date) {
    FindIterable<Document> find = this.getCollection(ConstantMongodb.DATABASE_ANALYSIS, collName)
        .find(new Document(Constant.DATE, date)).projection(new Document("summary", 1));
    find.maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS);
    MongoCursor<Document> cursor = find.iterator();
    List<Document> docList = new ArrayList<>();
    while (cursor.hasNext()) {
      docList.add(cursor.next());
    }
    List<Document> summaryDocList = utils.summaryByDate(docList);
    if (summaryDocList.isEmpty()) return new Document();
    return summaryDocList.get(0);
  }
}