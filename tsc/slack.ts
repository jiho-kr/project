import { WebClient } from '@slack/web-api';
import * as fs from 'fs';
import * as _ from 'lodash';
import { constant } from '../constants/constant';

export class Slack {
  private web = new WebClient(constant.SLACK_TOKEN);

  async postMessage(message: any) {
    await this.web.chat.postMessage(message);
  }

  async sendNotiMessage(channel: string, username: string, text: string, url?: string) {
    await this.web.chat.postMessage({
      channel,
      text: '',
      icon_emoji: ':notify:',
      username,
      attachments: [
        { text,
          color: '#FFC922',
          actions: url ? [
            { type: 'button',
              text: ':mag:',
              url
            }
          ] : []
        }
      ]
    });
  }

  async postMetricCheckMessage(area: string, message: any) {
    return this.web.chat.postMessage({
      channel: constant.SLACK_CHANNEL,
      text: '',
      icon_emoji: ':male-detective:',
      username: constant.SLACK_NAME,
      attachments: [
        { text: ':query: *Checked Metric collection status*',
          color: '#FFC922',
          fields: [
            {
              title: 'Area',
              value: area,
              short: true
            },
              ..._.map(message, (count, title) => {
              return { title, value: `${count}`, short: true };
            })
          ]
        }
      ]
    });
  }

  async postSlowQueryMessage(title: string, area: string, cmdType: string, database: string, msg: string,
                             execTime: number, pattern: string) {
    return this.web.chat.postMessage({
      channel: constant.SLACK_CHANNEL_SLOWQUERY,
      text: '',
      icon_emoji: ':female-detective:',
      username: constant.SLACK_NAME,
      attachments: [
        { text: title,
          color: '#4B8B3B',
          fields: [
            {
              title: 'Area',
              value: area,
              short: true
            },
            {
              title: 'Command',
              value: cmdType,
              short: true
            },
            {
              title: 'Database',
              value: database,
              short: true
            },
            {
              title: 'Exec Time',
              value: `${execTime.toLocaleString()} ms`,
              short: true
            },
            {
              title: 'Message',
              value: msg,
              short: false
            }
          ],
          actions: [
            { type: 'button',
              text: ':mag: Dashboard',
              url: `${constant.DASHBOARD_URL}/app/SlowQuery/Detail?pattern=${pattern}`
            },
            { type: 'button',
              text: ':jira: Send To JIRA',
              url: `${constant.DASHBOARD_URL}/app/SlowQuery/Detail?pattern=${pattern}&jira=true`
            },
            { type: 'button',
              text: ':mute: Mute',
              url: `${constant.DASHBOARD_URL}/app/SlowQuery/Detail?pattern=${pattern}&mute=true`
            }
          ]
        }
      ]
    });
  }

  async sendFile(channels: string, filename: string, title: string, filetype: string): Promise<void> {
    await this.web.files.upload({ channels, file: fs.createReadStream(filename), filename, title, filetype });
  }
}

export const slack = new Slack();
