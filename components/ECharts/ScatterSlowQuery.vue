<template>
  <div class="echarts">
    <IEcharts :option="option" :notMerge="true" @dblclick="onDblclick"/>
  </div>
</template>

<script>
import _ from 'lodash';
import IEcharts from 'vue-echarts-v3/src/full.js'

export default {
  components: { IEcharts },
  // { key: []}
  props: [ 'data', 'dblEvent' ],
  data () {
    return {
      option: {}
    }
  },
  methods: {
    initData () {
      this.option = {
        toolbox: { show: false },
        tooltip : {
          trigger: 'axis',
          showDelay: 0,
          formatter: (params) => {
            return `${params[0].seriesName}<br/>Max: ${params[0].value[1].toLocaleString()}\
            <br/>Avg: ${params[0].value[0].toLocaleString()}\
            <br/>Count: ${params[0].value[2].toLocaleString()}`;
          },  
          axisPointer: {
            show: true,
            type : 'cross',
            lineStyle: {
              type : 'dashed',
              width : 1
            }
          }
        },
        xAxis: [
          {
            type: 'value',
            scale: true,
            name: 'Avg Exec Time',
            axisLabel: {
              formatter: '{value} ms'
            }
          }
        ],
        yAxis: [
          {
            type: 'value',
            scale: true,
            name: 'Max Exec Time',
            axisLabel: {
              formatter: '{value} ms'
            }
          }
        ],
        legend: {
          show: false,
          data: Object.keys(this.data)
        },
        series: _.map(this.data, (data, name) => {
          return { name, type: 'scatter', data,
                   symbolSize: (value) => {
                     const size = value[2] / 500;
                     return size < 5 ? 5 : (size > 75 ? 75 : size);
                   }
          }
        })
      };
    },
    onDblclick(param) {
      if (param.componentType !== `series` || !param.seriesName || !this.dblEvent) return;
      this.$events.fire(this.dblEvent, { data: param.data });
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

<style scoped>
  .echarts {
    width: 100%;
    height: 500px;
  }
</style>