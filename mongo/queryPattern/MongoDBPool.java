package com.ea.asiacentraltech.thoth.mongodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.bson.Document;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;

public class MongoDBPool extends GenericObjectPool<MongoDB> {
  public MongoDBPool(PooledObjectFactory<MongoDB> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public MongoDBPool(PooledObjectFactory<MongoDB> factory,
                     GenericObjectPoolConfig config) {
    super(factory, config);
  }

  /**
   * FindOne
   * @param Database
   * @param Collection
   * @param Query
   * @param Fields
   * @return
   * @throws Exception
   */
  public Document findOne(String database,
                          String collection,
                          Document query,
                          Document fields) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.docFindOne(database, collection, query, fields);
      
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public Document findOne(String database,
                          String collection,
                          Document query,
                          Document fields,
                          Document sort) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.getCollection(database, collection)
                    .find(query)
                    .projection(fields)
                    .sort(sort)
                    .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                    .first();
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public void updateOne(String database,
                        String collection,
                        Document filter,
                        Document update,
                        UpdateOptions options) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      mongoDB.getCollectionPrimary(database, collection)
             .updateOne(filter, update, options);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public List<Document> find(String database,
      String collection,
      Document query,
      Document fields,
      Document sort) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      List<Document> docList = new ArrayList<>();
      MongoCursor<Document> cursor = mongoDB.getCollection(database, collection)
                                            .find(query)
                                            .projection(fields)
                                            .sort(sort)
                                            .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                                            .iterator();
      while (cursor.hasNext()) {
        docList.add(cursor.next());
      }
      return docList;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public List<Document> find(String database,
                             String collection,
                             Document query,
                             Document fields,
                             Document sort,
                             int limit) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      List<Document> docList = new ArrayList<>();
      MongoCursor<Document> cursor = mongoDB.getCollection(database, collection)
                                            .find(query)
                                            .projection(fields)
                                            .sort(sort)
                                            .limit(limit)
                                            .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                                            .iterator();
      while (cursor.hasNext()) {
        docList.add(cursor.next());
      }
      return docList;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public List<Document> find(String[] databases,
                             String[] collections,
                             Document query,
                             Document fields,
                             Document sort,
                             int limit,
                             int skip) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      List<Document> docList = new ArrayList<>();
      for (String database : databases) {
        for (String collection: collections) {
          MongoCursor<Document> cursor = mongoDB.getCollection(database, collection)
                                                .find(query)
                                                .projection(fields)
                                                .sort(sort)
                                                .limit(limit)
                                                .skip(skip)
                                                .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                                                .iterator();
          while (cursor.hasNext()) {
            docList.add(cursor.next());
          }
        }
      }
      return docList;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public void insertOne(String database,
                        String collection,
                        Document doc) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      mongoDB.getCollectionPrimary(database, collection)
             .withWriteConcern(WriteConcern.ACKNOWLEDGED)
             .insertOne(doc);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public void deleteOne(String database,
                        String collection,
                        Document query) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      mongoDB.getCollectionPrimary(database, collection)
             .deleteOne(query);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public Set<String> getCollcetionNames(String database) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.getCollcetions(database);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public Set<String> getCollcetionNames(String[] databases) throws Exception {
    if (databases != null && databases.length == 1)
      return getCollcetionNames(databases[0]);
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      Set<String> collNames = new HashSet<>();
      for (String database : databases)
        collNames.addAll(mongoDB.getCollcetions(database));
      return collNames;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public Set<String> getDatbaseNames() throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.getDatabaseNames();
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public Document command(String database,
                          Document command) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.runCommand(database, command);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public void dropDatabase(String database) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      mongoDB.dropDatabase(database);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public List<String> distinct(String database,
                               String collection,
                               String key) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      return mongoDB.distinct(database, collection, key);
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }

  public List<Document> aggregate(String[] databases,
                                  String[] collections,
                                  List<Document> pipe) throws Exception {
    MongoDB mongoDB = null;
    try {
      mongoDB = this.borrowObject();
      List<Document> docList = new ArrayList<>();
      for (String database : databases) {
        for (String collection : collections) {
          MongoCursor<Document> cursor = mongoDB.getCollection(database, collection)
                                                .aggregate(pipe)
                                                .maxTime(ConstantMongodb.MONGODB_CURSOR_MAX_SECS, TimeUnit.SECONDS)
                                                .iterator();
          while (cursor.hasNext()) {
            docList.add(cursor.next());
          }
        }
      }
      return docList;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
  
  public List<Document> count(String[] databases,
      String[] collections,
      Document query) throws Exception {
    MongoDB mongoDB = null;
    List<Document> docList = new ArrayList<>();
    try {
      mongoDB = this.borrowObject();
      for (String database : databases) {
        for (String collection: collections) {
          docList.add(new Document().append("database", database)
                                    .append("collection", collection)
                                    .append("count", mongoDB.getCollection(database, collection).count(query)));
        }
      }
      return docList;
    } finally {
      if (mongoDB != null) this.returnObject(mongoDB);
    }
  }
}
