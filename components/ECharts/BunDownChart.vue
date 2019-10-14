<template>
  <div class="echarts">
    <IEcharts :notMerge="notMerge" :option="graphOption" />
  </div>
</template>

<script>
import IEcharts from 'vue-echarts-v3/src/lite.js';
import 'echarts/lib/chart/line';
import { secondsToEstimateHour } from '@/utils/utils';

export default {
  components: { IEcharts },
  props: [ 'data' ],
  data() {
    return {
      notMerge: true,
      ins: null,
      echarts: null,
      graphOption: {}
    }
  },
  methods: {
    parseData() {
      if (!this.data) return;
      const xAxis = this.data.map(d => d.date);
      const legend = ['Time Spent', 'Remaining Values'];
      const series = [];
      series.push({
        name: legend[0],
        type: 'line',
        data: this.data.map(d => +d.timeSpentSum[0].replace('h', '')),
        showSymbol: false,
        hoverAnimation: true
      });
      series.push({
        name: legend[1],
        type: 'line',
        data: this.data.map(d => +d.remainningTimeEstimateSum[0].replace('h', '')),
        showSymbol: false,
        hoverAnimation: true
      });
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
            left: 80,
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
            data: xAxis,
            sort: 'none'
          }
        ],
        yAxis: [
          {
            type: 'value',
            axisLabel: {
              interval: 0,
              formatter: function (value) {
                return secondsToEstimateHour(value);
              }
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
        series
      };
    }
  },
  watch: {
    data: function() {
      this.parseData();
    }
  },
  mounted() {
    this.parseData();
  }
}
</script>

<style scoped>
  .echarts {
    width: 100%;
    height: 300px;
  }
</style>