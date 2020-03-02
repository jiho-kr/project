import { Di, ObjectFactory } from '@island/island';
import * as Bluebird from 'bluebird';
import * as _ from 'lodash';
import * as puppeteer from 'puppeteer';
import { constant } from '../constants/constant';
import * as nowConstant from '../constants/nexon.now.constant';
import { PuppeteerService } from '../service/puppeteer.service';
import { logger } from '../util/logger';

@Di.bindTransientClass
export class NexonNowCrawler {
  private puppeteerService: PuppeteerService;
  private page: puppeteer.Page;

  constructor() {
    this.puppeteerService = ObjectFactory.get<PuppeteerService>(PuppeteerService);
  }

  async run(func: () => Promise<void>) {
    await this.puppeteerService.runPage(async (page: puppeteer.Page) => {
      this.page = page;
      await this.login();
      await func();
    });
  }

  async getHostList(service: nowConstant.Service,
                    group: nowConstant.Group.Fo4Group | nowConstant.Group.Fo4AWSGroup | nowConstant.Group.Fo4TestGroup)
  : Promise<string[]> {
    const url = `${constant.NEXON_NOW_URL}/perf/${service}?host_group=${group}&align_y=N`;
    const selector = '#hosts-container a';
    logger.debug('getHost ' + url);
    try {
      await this.page.goto(url);
      await this.waitForSelector(this.page, selector, 3000);
      const links = await this.page.$$eval(selector,
        (am: any) => am.filter((e: any) => e.href).map((e: any) => e.href));
      return links.map((link: string) => link.substr(link.lastIndexOf('/') + 1));
    } catch (e) {
      logger.warning(`getHostList ${url} ${e.toString()}`);
    }
    return [];
  }

  async getMetricCategory(service: nowConstant.Service, hostName: string): Promise<any[]> {
    const url = `${constant.NEXON_NOW_URL}/perf/${service}/${hostName}`;
    try {
      await this.page.goto(url);
      await this.waitForSelector(this.page, 'ul', 3000);
      // tslint:disable-next-line
      const counter_metrics = await this.page.evaluate(() => counter_metrics);
      return _.reduce(counter_metrics, (category, v) => {
        category.push(...v.map(info => info.metric));
        return category;
      }, []).filter(Boolean);
    } catch (e) {
      logger.warning(`getMetricCategory ${url} ${e.toString()}`);
    }
    return [];
  }

  async getMetric(service: nowConstant.Service,
                  hostname: string,
                  metricTitle: string,
                  metric: string,
                  startDate: Date,
                  endDate: Date): Promise<{[metric: string]: {[ts: number]: number }}> {
    const url = `${constant.NEXON_NOW_URL}/api/perf/5m_max`;
    const startTime = +startDate / 1000;
    const endTime = +endDate / 1000;
    const timeRange = `${startTime}-${endTime}`;
    const uniqhostname = encodeURIComponent(`${service}/${hostname}`);
    const opts = `uniqhostname=${uniqhostname}&metric=${encodeURIComponent(metric)}`;
    // logger.debug('getMetric ' + `${hostname} ${url}/${timeRange}?${opts}`);
    let result: {[metric: string]: {[ts: number]: number }} = {[metricTitle]: {}};
    await this.puppeteerService.runPage(async (page: puppeteer.Page) => {
      try {
        await page.goto(`${url}/${timeRange}?${opts}`, { timeout: 60000 });
        await this.waitForSelector(page, 'pre', 3000);
      } catch (e) {
        // when no data, throw error
        // logger.warning(`${hostname} ${url}/${timeRange}?${opts} ${e.toString()}`);
        return;
      }
      const perf = await page.evaluate(() => {
        return Array.from(document.querySelectorAll('pre')).reduce((perf, elem) => {
          (elem.innerHTML.trim().split('\n') || []).forEach(data => {
            const info = data.split('\t');
            if (!info || info.length !== 2) return;
            perf[info[0]] = +info[1];
          });
          return perf;
        }, {});
      });
      result = { [metricTitle]: perf };
    });
    return result;
  }

  async getSlowQuery(service: nowConstant.Service, start: number, end: number, pageNum: number = 1) {
    const url = `${constant.NEXON_NOW_URL}/log/dbslowlog/${service}`;
    const opts = `sDate=${start}&eDate=${end}&page=${pageNum}`;
    let logs: any[] = [];
    await this.puppeteerService.runPage(async (page: puppeteer.Page) => {
      let data: any;
      try {
        await page.goto(`${url}?${opts}`, { timeout: 60000 });
        await Bluebird.delay(3000);
        const _data = await page.evaluate(() => Promise.resolve(_data));
        data = JSON.parse(_data);
      } catch (e) {
        return;
      }
      if (_.get(data, 'status')) return;
      const hits: any = _.get(data, 'hits', {});
      const totalCount: number = _.get(hits, 'total', 0);
      logs = _.get(hits, 'hits', []);
      if (pageNum === 1 && totalCount > 20) {
        const maxPage = Math.floor(totalCount / 20) > 500 ? 500 : Math.floor(totalCount / 20);
        for (const pages of _.chunk(_.range(2, maxPage), 10)) {
          const nextPageLogs: any[][] = await Promise.all(pages.map(page =>
            this.getSlowQuery(service, start, end, page)));
          logs.push(..._.flatten(nextPageLogs));
        }
      }
    });
    return logs;
  }

  private async login() {
    const clickButton = '[name="btnLogin"]';
    await this.page.goto(constant.NEXON_NOW_URL);
    await this.waitForSelector(this.page, clickButton, 1000);
    try {
      await this.page.type('#txtUserID', constant.NEXON_NOW_USERNAME);
    } catch (e) {
      return;
    }
    await this.page.type('#txtPassword', constant.NEXON_NOW_PASSWORD);
    await this.page.click(clickButton);
    await this.waitForSelector(this.page, 'h2', 1000);
  }

  private async waitForSelector(page: puppeteer.Page, tag: string, timeout: number): Promise<void> {
    try {
      await page.waitForSelector(tag, { timeout });
    } catch (e) {
      // ignore
    }
  }
}
