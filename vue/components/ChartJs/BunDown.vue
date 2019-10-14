<script>
import _ from 'lodash';
import moment from 'moment';
import { Line } from 'vue-chartjs';
import { formatBytes } from '@/utils/utils';
import 'chartjs-plugin-colorschemes';

export default {
  extends: Line,
  props: ['data', 'sprint'],
  data() {
    return {
    }
  },
  methods: {
    parseData() {
      const labels = [];
      const start = moment(this.sprint.startDate);
      const end = moment(this.sprint.endDate);
      const totalDay = moment.duration(end.diff(start)).asDays();
      const workDay = _.range(totalDay).map(n => {
        const day = start.add(n === 0 ? n : 1, 'days').day();
        labels.push(start.format('YYYY-MM-DD'));
        return day !== 0 && day !== 6;
      }).filter(Boolean).length;
      const remain = this.getData(labels, 'remainningTimeEstimateSum');
      const timeSpent = this.getData(labels, 'timeSpentSum');
      return {
        labels,
        datasets: [
          {
            label: 'Time Spent',
            data: timeSpent,
            pointRadius: 3,
            fill: false,
            steppedLine: true
          },
          {
            label: 'Remaining Values',
            data: remain,
            pointRadius: 3,
            fill: false,
            steppedLine: true
          },
          {
            label: 'Guide Line',
            data: this.getGuidLine(labels, workDay, remain[0]),
            pointRadius: 0,
            fill: false
          },
        ]
      };
    },
    getGuidLine(labels, workDay, startData) {
      const avg = startData / workDay;
      let estimate = startData;
      return labels.map((date, index) => {
        const day = moment(date).day();
        estimate -= index === 0 || day === 0 || day === 6 ? 0 : avg;
        return index === labels.length - 1 ? 0 : estimate;
      });
    },
    getData(labels, key) {
      const nowAt = moment().unix();
      const data = this.data.reduce((data, d) => {
        const date = moment(d.date).format('YYYY-MM-DD');
        const sumValue = _.get(d, key);
        data[date] = +sumValue[sumValue.length - 1].replace('h', '');
        return data;
      }, {});
      let pre = 0;
      return labels.map(date => {
        let value = _.get(data, date);
        if (value === undefined) {
          if (nowAt < moment(date).unix()) return;
          value = pre;
        } else {
          pre = value;
        }
        return value;
      })
    },
    renderLineChart() {
      const chartData = this.parseData();
      this.renderChart(chartData, {
        responsive: false,
        maintainAspectRatio: true,
        parseTime : false,
        scales: {
          yAxes: [{
            ticks:{
              callback:(v) => formatBytes(v),
              beginAtZero: true
            },
            gridLines: {
              display: true
            }
          }],
          xAxes: [{
            gridLines: {
              display: false
            }
          }]
        },
        legend: {
          position: 'bottom',
          labels: {
            fontSize: 9
          }
        },
        plugins: {
          datalabels: {
            display: false
          },
          colorschemes: {
            scheme: 'tableau.Traffic9'
          }
        }
      });
    }
  },
  watch: {
    data: function() {
      this.renderLineChart();
    }
  },
  mounted() {
    this.renderLineChart();
  }
}
</script>