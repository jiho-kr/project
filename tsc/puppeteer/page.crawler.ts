import { Di } from '@island/island';
import * as fs from 'fs';
import * as puppeteer from 'puppeteer';
import { logger } from '../util/logger';
import { InflightRequests } from './InflightRequests';

@Di.bindTransientClass
export class PageCrawler {
  private browser: puppeteer.Browser;
  private page: puppeteer.Page;

  constructor() {}

  getImagePath() {
    return `${__dirname}/../../image/`;
  }

  async getScreenShot(page: string, waitForSelector?: string): Promise<{imageName: string, imagePath: string}> {
    const imageName = `${Date.now()}.png`;
    const imagePath = `${this.getImagePath()}${imageName}`;
    await this.run(async () => {
      await this.page.setViewport({ width: 2048, height: 1536 });
      const tracker = new InflightRequests(this.page);
      await this.page.goto(page, {}).catch(e => {
        const inflight = tracker.inflightRequests();
        const fail = tracker.failedRequests();
        console.log('Remaining Requests:\n' + inflight.map(req => req.url()).join('\n'));
        console.log('Failed Requests:\n' + fail.map(req => req.url()).join('\n'));
        throw new Error(e.message);
      });
      tracker.dispose();
      if (waitForSelector) {
        await this.page.waitForSelector(waitForSelector);
      } else {
        await this.page.waitFor(3000);
      }
      await this.page.screenshot({path: imagePath, fullPage: true});
    });
    return { imageName, imagePath };
  }

  async removeImage(fileName: string) {
    try {
      await fs.unlinkSync(`${this.getImagePath()}${fileName}`);
    } catch (e) {
      logger.warning('fail removeImage', e);
    }
  }

  private async run(func: () => Promise<void> ) {
    if (this.browser) await this.browser.close();
    this.browser = await puppeteer.launch({
      headless: true,
      ignoreHTTPSErrors: true,
      args: ['--no-sandbox',
             '--disable-setuid-sandbox',
             '--disable-dev-shm-usage']
    });
    this.page = await this.browser.newPage();
    try {
      await func();
    } catch (e) {
      logger.warning('PageCrawler error', e);
      throw e;
    } finally {
      await this.page.close();
      await this.browser.close();
    }
  }
}
