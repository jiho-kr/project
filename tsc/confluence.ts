import * as _ from 'lodash';
import * as request from 'superagent';
import { logger } from '../util/logger';

const config = {
  host: 'http://confluence.url',
  url: 'http://confluence.url/rest/api/content',
  uid: 'id',
  pwd: '*******'
};

interface ConfluencePage {
  version: number;
  by: string;
  userid: string;
  when: string;
  title: string;
  space: string;
  path: string;
  body: string;
}

export async function getPage(pid: number): Promise<ConfluencePage> {
  let response: request.Response;
  try {
    response = await request.get(`${config.url}/${pid}?expand=space,version,ancestors,body.storage`)
                            .auth(config.uid, config.pwd);
  } catch (e) {
    logger.warning('Confluence getPage ', _.get(e, 'response.body'));
    return;
  }
  const body = response.body;
  return {
    version: +body.version.number,
    by: body.version.by.displayName,
    userid: body.version.by.username,
    when: body.version.when,
    title: body.title,
    space: body.space.name,
    path: body.ancestors.reduce((p: any, c: any) => { p.push(c.title); return p; }, []).join('/'),
    body: body.body.storage.value
  };
}

export async function createConfluencePage(pos: { space: string, id: number },
                                           title: string, body: string): Promise<string> {
  try {
    const res = await request.post(config.url).auth(config.uid, config.pwd)
                             .set('Content-Type', 'application/json')
                             .send({type: 'page',
                                    title,
                                    space: { key: pos.space },
                                    ancestors: [ { id: pos.id } ],
                                    body: {
                                      storage: {
                                        value: body,
                                        representation: 'storage'
                                      }
                                    }});
    return res.body.id;
  } catch (e) {
    logger.warning('Confluence postChildPage ', _.get(e, 'response.body'));
  }
}

export async function modifyConfluencePage(pid: number, title: string, body: string, version: number): Promise<void> {
  try {
    await request.put(`${config.url}/${pid}`)
                 .auth(config.uid, config.pwd)
                 .set('Content-Type', 'application/json')
                 .send({ id: pid,
                         type: 'page',
                         title,
                         body: {
                           storage: {
                             value: body,
                             representation: 'storage'
                           }
                         },
                         version: { number: version }
                       });
  } catch (e) {
    logger.warning('Confluence putPage ', _.get(e, 'response.body'));
  }
}

export async function uploadFile(pid: number, filePath: string) {
  try {
    await request.post(`${config.url}/${pid}/child/attachment`)
                 .auth(config.uid, config.pwd)
                 .set('Content-Type', 'multipart/form-data')
                 .set('X-Atlassian-Token', 'no-check')
                 .attach('file', filePath);
  } catch (e) {
    logger.warning('Confluence uploadFile ', _.get(e, 'response.body'));
  }
}

export async function getChildPages(pid: number): Promise<{ id: string, title: string, link: string }[]> {
  try {
    const res = await request.get(`${config.url}/${pid}/child/page?limit=500`).auth(config.uid, config.pwd)
                             .set('Content-Type', 'application/json');
    return _.get(res.body, 'results', []).map((result: any) =>
      _.merge(_.pick(result, ['id', 'title']), { link: `${config.host}${_.get(result, '_links.webui')}` }));
  } catch (e) {
    logger.warning('Confluence getChildPages ', _.get(e, 'response.body'));
  }
}

export async function searchPages(opts: any): Promise<{ id: string, title: string, link: string }[]> {
  try {
    _.merge(opts, { limit: 100 });
    const res = await request.get(`${config.url}`).auth(config.uid, config.pwd)
                             .query(opts)
                             .set('Content-Type', 'application/json');
    return _.get(res.body, 'results', []).map((result: any) =>
    _.merge(_.pick(result, ['id', 'title']), { link: `${config.host}${_.get(result, '_links.webui')}` }));
  } catch (e) {
    logger.warning('Confluence searchPages', _.get(e, 'response.body'));
  }
}
