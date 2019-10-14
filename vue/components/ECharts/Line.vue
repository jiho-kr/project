<template>
  <div class="echarts">
    <IEcharts :option="graphOption" :notMerge="notMerge"/>
  </div>
</template>

<script>
import _ from 'lodash';
import moment from 'moment';
import IEcharts from 'vue-echarts-v3/src/lite.js';
import 'echarts/lib/chart/line';

export default {
  components: { IEcharts },
  props: [ 'data', 'notMerge', 'format' ],
  data() {
    return {
      ins: null,
      echarts: null,
      graphOption: {}
    }
  },
  methods: {
    parseData() {
      // data = { [series: string] : { [timestamp: number] : value ... } }
      const legend = Object.keys(this.data);
      const seriesDatas = legend.reduce((def, name) => _.merge(def, { [name]: [] }), {} );
      const convertData = {}; // { '01.00:00': { KR: 3, TH: 5 ..}}
      _.forEach(this.data, (countryInfo, country) => {
        _.forEach(countryInfo, (value, timestamp) => {
          const cutMinute = Math.floor(timestamp / 60000) * 60000;
          convertData[cutMinute] = convertData[cutMinute] || {};
          convertData[cutMinute][country] = value;
        })
      });
      const tsList = Object.keys(convertData).sort();
      tsList.forEach(time => {
        legend.forEach(area => {
          seriesDatas[area].push(convertData[time][area]);
        })
      });
      const xAxisData = tsList.map(time => moment(+time).format(this.format || 'HH:mm'));
      this.graphOption = {
        tooltip : { trigger: 'axis' },
        legend: {
          data: legend,
          left: 'right',
          top: 'middle',
          orient: 'vertical',
          padding: 5,
          itemHeight: 8,
          textStyle: {
            fontSize: 10
          }
        },
        toolbox: {
          show : false,
          feature : {
            magicType : {show: true, type: ['line', 'bar']},
            saveAsImage : {show: true}
          }
        },
        grid: [{
            show: true,
            top: 10,
            left: 50,
            right: 100,
            bottom: 20
        }],
        dataZoom : {
          show : false,
          realtime : false,
          start : 0,
          end : 100
        },
        calculable: true,
        xAxis: [
          {
            type: 'category',
            boundaryGap: true,
            data: xAxisData,
            sort: 'none'
          }
        ],
        yAxis: [
          {
            type: 'value',
            axisLabel: {
              interval: 0,
              formatter: '{value}'
            },
            boundaryGap: [0, 0],
            splitLine: {
              show: true,
              lineStyle: {
                color: ['#eee6c4', '#ffd400'],
                type: 'dotted'
              }
            }
          }
        ],
        series: legend.map(name => {
          return {
            name,
            type: 'line',
            data: seriesDatas[name],
            showSymbol: false,
            hoverAnimation: true,
            markPoint : {
              data : [ { type : 'max', name: 'Max' } ]
            }
          };
        })
      };
    }
  },
  watch: {
    data: function() {
      this.parseData();
    }
  },
  mounted() {
    this.notMerge = !!(this.notMerge);
    this.parseData();
  }
}
</script>

<style scoped>
  .echarts {
    height: 300px;
    margin: 0 auto;
  }
  h1, h2 {
    font-weight: normal;
  }
  ul {
    list-style-type: none;
    padding: 0;
  }
  li {
    display: inline-block;
    margin: 0 10px;
  }
  a {
    color: #42b983;
  }
</style>