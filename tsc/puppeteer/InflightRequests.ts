import { page } from 'puppeteer';

export class InflightRequests {
  private _page: page;
  private _requests: Set<any>;
  private _fails: Set<any>;

  constructor(page: page) {
    this._page = page;
    this._requests = new Set();
    this._fails = new Set();
    this._onStarted = this._onStarted.bind(this);
    this._onFinished = this._onFinished.bind(this);
    this._onFailed = this._onFailed.bind(this);
    this._page.on('request', this._onStarted);
    this._page.on('requestfinished', this._onFinished);
    this._page.on('requestfailed', this._onFailed);
  }

  _onStarted(request) { this._requests.add(request); }
  _onFinished(request) { this._requests.delete(request); }
  _onFailed(request) { this._fails.add(request); }

  inflightRequests() { return Array.from(this._requests); }
  failedRequests() { return Array.from(this._fails); }

  dispose() {
    this._page.removeListener('request', this._onStarted);
    this._page.removeListener('requestfinished', this._onFinished);
    this._page.removeListener('requestfailed', this._onFailed);
  }
}
