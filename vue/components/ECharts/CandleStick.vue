<template>
  <div class="echarts">
    <IEcharts :option="option" :notMerge="true"/>
  </div>
</template>

<script>
import moment from 'moment';
import IEcharts from 'vue-echarts-v3/src/full.js'
const Tableau20 = ['#4E79A7', '#A0CBE8', '#F28E2B', '#FFBE7D', '#59A14F', '#8CD17D', '#B6992D', '#F1CE63', '#499894', '#86BCB6', '#E15759', '#FF9D9A', '#79706E', '#BAB0AC', '#D37295', '#FABFD2', '#B07AA1', '#D4A6C8', '#9D7660', '#D7B5A6'];

export default {
  components: { IEcharts },
  // data: [{ area, min, max, start, end, date }]
  props: [ 'data'],
  data () {
    return {
      option: {}
    }
  },
  methods: {
    initData () {
      const legend = [];
      const xAxis = [];
      const convertData = {};
      this.data.forEach(doc => {
        const { area, min, max, start, end, date } = doc;
        const dateStr = moment(date).format('YYYY-MM-DD');
        if (legend.indexOf(area) === -1) legend.push(area);
        if (xAxis.indexOf(dateStr) === -1) xAxis.push(dateStr);
        convertData[area] = convertData[area] || {};
        convertData[area][dateStr] = [ start, end, min, max ];
      });
      xAxis.sort();
      let colorNum = 0;
      this.option = {
        tooltip: {
          trigger: 'axis',
          formatter: function (params) {
              let res = params[0].seriesName + ' ' + params[0].name;
              res += '<br/>  Open: ' + params[0].value[1] + '  High: ' + params[0].value[4];
              res += '<br/>  Close: ' + params[0].value[2] + '  Low: ' + params[0].value[3];
              return res;
          }
        },
        legend: {
          data: legend
        },
        toolbox: {
          show: false
        },
        dataZoom: {
          show: true,
          realtime: true,
          start: 0,
          end: 100
        },
        xAxis: [
          {
            type: 'category',
            boundaryGap: true,
            axisTick: { onGap: false },
            splitLine: { show: false },
            data: xAxis
          }
        ],
        yAxis: [
          {
            type: 'value',
            scale: true,
            boundaryGap: [0.01, 0.01]
          }
        ],
        series: legend.map(name => {
          return { name, type: 'k', data: xAxis.map(dateStr => convertData[name][dateStr] || [0,0,0,0]),
                   itemStyle: {
                     normal: {
                       color: Tableau20[colorNum++],
                       color0: Tableau20[colorNum++],
                     }
                   }};
          })
      };
    }
  },
  watch: {
    data: function() {
      this.initData();
    }
  },
  mounted() {
    this.initData();
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
  .echarts {
    width: 100%;
    height: 450px;
  }
</style>