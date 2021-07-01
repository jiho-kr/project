import type { Browser, Page } from 'puppeteer';
import { Injectable, Logger } from '@nestjs/common';
import randomUseragent from 'random-useragent';
import _ from 'lodash';
import fs from 'fs';
import dayjs from 'dayjs';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

import { ConfigService } from '../../common/config/config.service';
import { InjectBrowser } from '../puppeteer/puppeteer.decorator';
import { FirebaseSearchDto } from './dto/firebase-serach.dto';
import { SlackService } from '../slack/slack.service';
import { MobileCrashSummary } from './dto/mobile-crash-summary.dto';

dayjs.extend(timezone);
dayjs.extend(utc);

const MAX_RETRY_COUNT = 3;
const MAX_WAIT_TIME_FOR_COLLECT = 30000;

class SlackInformation {
  channel: string;
  thread_ts: string;
}

@Injectable()
export class CrawlerFirebaseService {
  private readonly GOOGLE_ACCOUNT: { id: string; password: string };

  constructor(
    @InjectBrowser() private readonly browser: Browser,
    public readonly configService: ConfigService,
    public readonly slackService: SlackService,
  ) {
    this.GOOGLE_ACCOUNT = {
      id: this.configService.get('GOOGLE_ID'),
      password: this.configService.get('GOOGLE_PASSWORD'),
    };
  }

  async getMobileCrashPerDay(
    firebaseSearchDto: FirebaseSearchDto,
  ): Promise<MobileCrashSummary[]> {
    if (_.isEmpty(firebaseSearchDto.apps)) {
      return;
    }
    Logger.log('[Crawler Firebase] Start');
    const firebaseUrl = (project: string, appId: string) =>
      [
        'https://console.firebase.google.com/u/0/project/',
        project,
        '/crashlytics/app/',
        appId,
        '/issues?type=crash&time=',
        [firebaseSearchDto.startAt, firebaseSearchDto.startAt + 86399999].join(
          ':',
        ),
      ].join('');
    const overviewUrl = (project: string, appId: string) =>
      [
        'https://console.firebase.google.com/u/0/project/',
        project,
        '/analytics/app/',
        appId,
        '/overview',
      ].join('');
    const overviewParam = (date: string, streamId: string) =>
      [
        'params%3D_u.date00%253D',
        date,
        '%2526_u.date01%253D',
        date,
        '%2526_u..dataFilters%253DstreamId%252C',
        streamId,
        '%252CEQ%252CDIMENSION%252C%252Ctrue',
      ].join('');

    const slackInformation = await this.getSlackInfo(
      firebaseSearchDto.slackChannel,
    );
    const page = await this.googleLogin(0, slackInformation);
    if (!page) {
      Logger.log('[Crawler Firebase] Failed Google Login');
      return [];
    }
    const paramDate = dayjs(firebaseSearchDto.startAt).utc().format('YYYYMMDD');
    const isoString = dayjs(firebaseSearchDto.startAt).utc().toISOString();

    const results = [];
    for (const app of firebaseSearchDto.apps) {
      const { summary } = await this.getCrashFromCrashlytics(
        page,
        firebaseUrl(app.project, [app.platform, app.appId].join(':')),
        slackInformation,
      );
      if (!summary) {
        continue;
      }

      const result = {
        region: app.region,
        project: app.platform,
        service: 'firebase',
        indexname: 'puppet',
        type: 'fo4mobileindex',
        at: isoString,
        crashratebyuser: 0,
        crashratebysession: 0,
      };
      if (app.platform === 'ios') {
        const { activeUsers } = await this.getActiveUsers(
          page,
          overviewUrl(app.project, [app.platform, app.appId].join(':')),
          overviewParam(paramDate, app.streamId),
          slackInformation,
        );
        result.crashratebyuser = +(
          (1 - (1 - summary.impactedDevicesCount / activeUsers)) *
          100
        ).toFixed(2);
      } else {
        result.crashratebyuser = +(
          (summary.impactedDevicesCount / summary.totalUsers) *
          100
        ).toFixed(2);
      }
      if (!_.isNaN(result.crashratebyuser)) {
        results.push(result);
      }
    }
    await page.close();
    await this.sendResult(results, slackInformation);

    Logger.log(`[Crawler Firebase] Done (${results.length})`);
    return results;
  }

  private async getActiveUsers(
    page: Page,
    overViewUrl: string,
    params: string,
    slackInformation?: SlackInformation,
  ): Promise<{ activeUsers: number }> {
    await page.goto(overViewUrl, { waitUntil: 'networkidle2', timeout: 0 });
    await page.waitForTimeout(2000);
    await this.sendScreenshot(overViewUrl, page, slackInformation);

    const url = page.url() + '&' + params;
    const urlPattern = /venus.*undefined_card_3/;
    const result = { activeUsers: undefined };

    page.on('requestfinished', async (request) => {
      if (request.method() === 'OPTIONS') {
        return;
      }
      const url = request.url();
      if (!urlPattern.test(url)) {
        return;
      }
      const response = request.response();
      const text = await response.text();
      const json = JSON.parse(text.split('\n')[1]);
      if (
        _.get(json, 'default.responses[0].metrics[1].id') !== 'active_users'
      ) {
        return;
      }
      result.activeUsers = +_.get(
        json,
        'default.responses[0].responseRows[0].metricCompoundValues[1].value',
      );
    });
    await page.goto(url, { waitUntil: 'networkidle2' });

    const now = Date.now();
    for (;;) {
      if (
        !_.isUndefined(result.activeUsers) ||
        Date.now() - now > MAX_WAIT_TIME_FOR_COLLECT
      ) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    await this.sendScreenshot(url, page, slackInformation);
    return result;
  }

  private async getCrashFromCrashlytics(
    page: Page,
    firebaseUrl: string,
    slackInformation?: SlackInformation,
  ): Promise<{ summary: any }> {
    const result = { summary: undefined };

    page.on('requestfinished', async (request) => {
      if (request.method() === 'OPTIONS') {
        return;
      }
      const url = request.url();
      if (url.includes('getFirebaseAppIntervalScalars')) {
        const response = request.response();
        try {
          const json = await response.json();
          result.summary = _.mapValues(json as any, (v: string) => +v);
        } catch {}
      }
    });
    await page.goto(firebaseUrl, { waitUntil: 'networkidle2', timeout: 0 });

    const now = Date.now();
    for (;;) {
      if (
        Object.values(result).filter(Boolean).length > 1 ||
        Date.now() - now > MAX_WAIT_TIME_FOR_COLLECT
      ) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    await this.sendScreenshot(firebaseUrl, page, slackInformation);

    return _.cloneDeep(result);
  }

  private async googleLogin(
    retryCount = 0,
    slackInformation?: SlackInformation,
  ): Promise<Page> {
    const page = await this.createPage('http://accounts.google.com');
    const tags = {
      email: 'input[type="email"]',
      password: 'input[type="password"]',
    };

    try {
      await page.waitForSelector(tags.email, { visible: true, timeout: 10000 });
    } catch {
      return page;
    }

    await this.sendScreenshot('Google Login #1', page, slackInformation);
    const emailForm = await page.$(tags.email);
    if (!emailForm) {
      return page;
    }

    await emailForm.type(this.GOOGLE_ACCOUNT.id);
    await emailForm.press(String.fromCharCode(13));

    try {
      await page.waitForSelector(tags.password, {
        visible: true,
        timeout: 10000,
      });
    } catch (e) {
      await this.sendScreenshot('Google Login #2', page, slackInformation);
      if (retryCount < MAX_RETRY_COUNT) {
        await page.close();
        Logger.warn(`[Google Login] rejected. retry....(${retryCount})`);
        return this.googleLogin(++retryCount, slackInformation);
      }
      return;
    }

    await this.sendScreenshot('Google Login #2', page, slackInformation);
    const passwordForm = await page.$(tags.password);
    await passwordForm.type(this.GOOGLE_ACCOUNT.password);
    await passwordForm.press(String.fromCharCode(13));

    await page.waitForTimeout(3000);

    await this.sendScreenshot('Google Login Done', page, slackInformation);

    await page.goto('http://accounts.google.com', {
      waitUntil: 'networkidle2',
      timeout: 0,
    });

    try {
      await page.waitForSelector('input[type="email"]', {
        visible: true,
        timeout: 10000,
      });

      await this.sendScreenshot('Google Logged', page, slackInformation);
      if (retryCount < MAX_RETRY_COUNT) {
        await page.close();
        Logger.warn(`[Google Login] rejected. retry....(${retryCount})`);
        return this.googleLogin(++retryCount, slackInformation);
      } else {
        return;
      }
    } catch {
      return page;
    }
    return page;
  }

  private async createPage(url: string): Promise<Page> {
    //Randomize User agent or Set a valid one
    const userAgent = randomUseragent.getRandom();
    const USER_AGENT =
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36';
    const UA = userAgent || USER_AGENT;
    const page = await this.browser.newPage();

    //Randomize viewport size
    await page.setViewport({
      width: 1920 + Math.floor(Math.random() * 100),
      height: 3000 + Math.floor(Math.random() * 100),
      deviceScaleFactor: 1,
      hasTouch: false,
      isLandscape: false,
      isMobile: false,
    });

    await page.setUserAgent(UA);
    await page.setJavaScriptEnabled(true);
    page.setDefaultNavigationTimeout(0);

    //Skip images/styles/fonts loading for performance
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      if (
        req.resourceType() == 'stylesheet' ||
        req.resourceType() == 'font' ||
        req.resourceType() == 'image'
      ) {
        req.abort();
      } else {
        req.continue();
      }
    });
    await page.evaluateOnNewDocument(() => {
      // Pass webdriver check
      Object.defineProperty(navigator, 'webdriver', {
        get: () => false,
      });
    });

    await page.evaluateOnNewDocument(() => {
      // Pass chrome check
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore: Unreachable code error
      window.chrome = {
        runtime: {},
        // etc.
      };
    });

    await page.evaluateOnNewDocument(() => {
      //Pass notifications check
      const originalQuery = window.navigator.permissions.query;
      return (window.navigator.permissions.query = (parameters) =>
        parameters.name === 'notifications'
          ? Promise.resolve({ state: Notification.permission })
          : (originalQuery(parameters) as any));
    });

    await page.evaluateOnNewDocument(() => {
      // Overwrite the `plugins` property to use a custom getter.
      Object.defineProperty(navigator, 'plugins', {
        // This just needs to have `length > 0` for the current test,
        // but we could mock the plugins too if necessary.
        get: () => [1, 2, 3, 4, 5],
      });
    });

    await page.evaluateOnNewDocument(() => {
      // Overwrite the `languages` property to use a custom getter.
      Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
      });
    });

    await page.evaluateOnNewDocument(() => {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore: Unreachable code error
      const newProto = navigator.__proto__;
      delete newProto.webdriver;
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore: Unreachable code error
      navigator.__proto__ = newProto;
    });

    await page.goto(url, { waitUntil: 'networkidle2', timeout: 0 });
    return page;
  }

  private async getSlackInfo(channel: string): Promise<SlackInformation> {
    if (!channel) {
      return;
    }
    const { ts } = await this.slackService.postMessage({
      channel,
      text: '',
      icon_emoji: ':female-detective:',
      username: 'DASHBOARD',
      attachments: [
        {
          text: 'Firebase로부터 Crash 정보를 수집합니다. [검증용]',
          color: '#FFC922',
        },
      ],
    });
    return { channel, thread_ts: ts };
  }

  private async sendScreenshot(
    title: string,
    page: Page,
    slackInformation: SlackInformation,
  ) {
    if (!slackInformation) {
      return;
    }
    const fileName = `${Date.now()}.png`;
    const path = `${__dirname}/../../../images/${fileName}`;
    await page.screenshot({ path });

    await this.slackService.sendFile(
      slackInformation.channel,
      path,
      fileName,
      title,
      'png',
      slackInformation.thread_ts,
    );
    try {
      fs.unlinkSync(path);
    } catch (e) {
      Logger.warn(e, 'failed removeImage');
    }
  }

  private async sendResult(
    mobileCrashSummary: MobileCrashSummary[],
    slackInformation: SlackInformation,
  ) {
    if (!slackInformation) {
      return;
    }

    await this.slackService.postMessage({
      channel: slackInformation.channel,
      icon_emoji: ':female-detective:',
      username: 'DASHBOARD',
      thread_ts: slackInformation.thread_ts,
      text: ['```', JSON.stringify(mobileCrashSummary, null, '\t'), '```'].join(
        '',
      ),
    });
  }
}
