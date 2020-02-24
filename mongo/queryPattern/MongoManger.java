package com.ea.asiacentraltech.thoth.mongodb;

import java.util.logging.Logger;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class MongoManger extends BasePooledObjectFactory<MongoDB> {
  private final Logger logger;
  private final String mongoHosts;
  private final String mongoId;
  private final String mongoPwd;

  public MongoManger(Logger logger, String host, String id, String pwd) {
    this.logger = logger;
    this.mongoHosts = host;
    this.mongoId = id;
    this.mongoPwd = pwd;
  }

  @Override
  public MongoDB create() throws Exception {
    MongoDB mongoDB = new MongoDB();
    mongoDB.bindLogger(logger);
    mongoDB.auth(mongoId, mongoPwd);
    mongoDB.multiConn(mongoHosts);
    return mongoDB;
  }

  @Override
  public PooledObject<MongoDB> wrap(MongoDB mongoDB) {
    return new DefaultPooledObject<>(mongoDB);
  }

  @Override
  public void destroyObject(PooledObject<MongoDB> p) throws Exception {
    try {
      p.getObject().close();
    } finally {
      super.destroyObject(p);
    }
  }
}
