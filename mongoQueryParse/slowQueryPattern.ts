const PATTERNS = [
  { command: 'insert', regexp: /\sinsert:\s".*",/, target: 'insert: '},
  { command: 'update', regexp: /\supdate:\s".*",/, target: 'update: ', query: 'q: '},
  { command: 'transactionStatusUpdate', regexp: /\s\_id:\sObjectId.'[a-f\d]{24}'.,\sstate:\s"pending"/},
  { command: 'transactionInvokeDoc', regexp: /\splanSummary:\sIXSCAN\s.\s\_id:\s1\s.\supdate:\s.\s.set:\s.\st:\sObjectId/, target: 'findandmodify: '},
  { command: 'transactionUpdate', regexp: /\s\_id:\sObjectId.'[a-f\d]{24}'.,\st:\sObjectId.'[a-f\d]{24}'.*\supdate:\s.*\s.set:.*\sObjectId/},
  { command: 'transactionPushHistory(common)', regexp: /\splanSummary:\sIDHACK\supdate:\s.\s.push:\s.\shistories:\s/},
  { command: 'transactionDateUpdate', regexp: /\s\_id:\sObjectId.'[a-f\d]{24}'.\s.\splanSummary:\sIDHACK\supdate:\s/},
  { command: 'checkViewedItems', regexp: /\s\_id:\s.\s.in:\s.\sObjectId.'[a-f\d]{24}'.*,\saccountid:\sObjectId.'[a-f\d]{24}'.*\supdate:\s.\s.set:\s.\sis\_new/},
  { command: 'checkMyOrdersNew', regexp: /\said:\sObjectId.'[a-f\d]{24}'.,\s\_id:\s.\s.in:\s.\sObjectId.'[a-f\d]{24}'.*.\supdate:\s.\s.set:\s.\sis\_new/},
  { command: 'getMore', regexp: /\sgetMore:\s\d/, target: 'collection: '},
  { command: 'find', regexp: /\sfind:\s".*",/, target: 'find: ', query: 'filter: ', sort: 'sort: '},
  { command: 'findAndModify', regexp: /\sfindandmodify:\s*".*",/i, target: 'findAndModify: ', query: 'query: '},
  { command: 'delete', regexp: /\sdelete:\s".*",/, target: 'delete: ', query: 'q: '},
  { command: 'aggregate', regexp: /\saggregate:\s".*",/, target: 'aggregate: ', query: 'pipeline: '},
  { command: 'count', regexp: /\scount:\s".*",/, target: 'count: "', query: 'query: '},
  { command: 'distinct', regexp: /\sdistinct:\s".*",/, target: 'distinct: ', query: 'query: '},
  { command: 'create', regexp: /\screate:\s*".*",/i, target: 'create: '},
  { command: 'drop', regexp: /\sdrop:\s*".*",/i, target: 'drop: '},
  { command: 'createIndex', regexp: /\screateIndexes:\s*".*",/i, target: 'createIndexes: '},
  { command: 'dropUser', regexp: /\sdropUser:\s*".*"/i, target: 'dropUser: '},
  { command: 'moveChunk', regexp: /\smoveChunk:\s".*",/, target: 'moveChunk: '},
  { command: 'moveChunkConfig', regexp: /\s\_configsvrMoveChunk:\s\d,/, target: 'ns: ' },
  { command: 'collStats', regexp: /\scollStats:\s".*"/, target: 'collStats: "'},
  { command: 'listCollections', regexp: /\slistCollections:\s\d/ },
  { command: 'serverStatus', regexp: /\sserverStatus:\s\d/}
];

const IGNORE_PATTERNS = [
  /\sfsyncUnlock:\s\d/,
  /\sfsync:\s\d/,
  /\sapplyOps:\s/
];

const VALUE_PATTERNS = [
  { regexp: /ObjectId.'[a-f\d]{24}\'./gi, str: '"ObjectId"' },
  { regexp: /new\sDate.[\d]{13}./gi, str: '"Date"' },
  { regexp: /\sTimestamp\s+\d*.\|+\d{1,}/gi, str: ' "Timestamp"'},
  { regexp: /\s.in:\s\[.*\]/gi, str: ' $in: []'},
  { regexp: /:\s\"[a-f\d]{24}\"/gi, str: ': "oid"'},
  { regexp: /:\s(\d|\.)*(\s|\,)/gi, str: ': 0, '},
  { regexp: /:\s\"(\W|\w|\s)*\"/gi, str: ': "-", '},
  { regexp: /\/[a-f\d]{24}\//gi, str: '"oid"' }
];

function getTarget(msg: string, s: string) {
  const index = msg.indexOf(s) + s.length;
  if (index === -1) return;
  const target = msg.substr(index + 1, msg.indexOf('"', index + 2) - index - 1);
  return target;
}

function extractObject(s: string) {
  let startCount = 0;
  let findObject = false;
  let msg = '';
  for (let i = 0; i < s.length; i++) {
    const c = s.charAt(i);
    if (c === '{') {
      startCount ++;
      findObject = true;
    } else if (c === '}') {
      if (findObject && --startCount === 0)
        return msg + c;
    }
    if (findObject) msg += c;
  }
}

export function getPattern(cmdType: string, db: string, msg: string): {
    cmdType: string;
    db: string;
    command: string;
    target: string;
    query: string;
  } {
  const info = { cmdType, db, command: '', target: '', query: '' };
  if (IGNORE_PATTERNS.map(p => p.test(msg)).filter(Boolean).length > 0) return;
  if (!(cmdType === null || cmdType === 'remove' || ['admin', 'local'].indexOf(db) !== -1 )) {
    for (const rule of PATTERNS) {
      if (rule.regexp.test(msg)) {
        info.command = rule.command;
        info.target = rule.target && getTarget(msg, rule.target);
        info.query = getQuery(msg, rule);
        break;
      }
    }
  }
  return info;
}

function parseQuery(q: string) {
  VALUE_PATTERNS.forEach(p => q = q.replace(p.regexp, p.str));
  return q;
}

function getQuery(msg: string, rule: { command: string;
                                       regexp: RegExp;
                                       target?: string;
                                       query?: string;
                                       sort?: string; }) {
  if (!rule.query) return;
  const query = extractObject(msg.substr(msg.indexOf(rule.query)));
  const sort = rule.sort && extractObject(msg.substr(msg.indexOf(rule.sort))) || '';
  return parseQuery(query + sort);
}
