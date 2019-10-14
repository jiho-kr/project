<template>
  <div class="echarts">
    <IEcharts :option="option" :notMerge="true" @dblclick="onDblclick"/>
  </div>
</template>

<script>
import _ from 'lodash';
import IEcharts from 'vue-echarts-v3/src/full.js'
const Tableau20 = ['#4E79A7', '#A0CBE8', '#F28E2B', '#FFBE7D', '#59A14F', '#8CD17D', '#B6992D', '#F1CE63', '#499894', '#86BCB6', '#E15759', '#FF9D9A', '#79706E', '#BAB0AC', '#D37295', '#FABFD2', '#B07AA1', '#D4A6C8', '#9D7660', '#D7B5A6'];

export default {
  components: { IEcharts },
  // category: ['10:10', '10:20' ]
  // bar: { a: [1,2,3], b:[1,2,3]}
  // Line: { c: [1,2,3] }
  props: [ 'category', 'bar', 'line', 'hideLegend', 'dblEvent', 'hideSeriesName'],
  data () {
    return {
      option: {}
    }
  },
  methods: {
    initData () {
      const yAxis = _.concat[[], Object.keys(this.bar || {}), Object.keys(this.line || {})];
      let colorNum = 0;
      const barSeries = _.map(this.bar, (data, name) => {
        return {
          name, type:'bar', stack: 'group',
          data,
          label: {
            show: false,
            color: '#000000'
          },
          itemStyle: {
            normal: {
              label : {show: false, position: 'insideRight'},
              color: Tableau20[colorNum++]
            }
          }
        }
      });
      const lineSeries = _.map(this.line, (data, name) => {
        return {
          name, type: 'line',
          data,
          datalabel: {
            show: true,
            color: '#000000'
          },
          itemStyle: {
            normal: {
              label : {show: false, position: 'insideRight'},
              color: Tableau20[colorNum++]
            }
          }
        }
      });
      const hideSeriesName = (this.hideSeriesName === true);
      this.option = {
          tooltip : {
            trigger: 'axis',
            axisPointer : { type : 'shadow'},
            formatter: function (params) {
              const colorSpan = color => '<span style="display:inline-block;margin-right:5px;border-radius:10px;width:9px;height:9px;background-color:' + color + '"></span>';
              const name = (params[0] || {}).name;
              const series = params.filter(d => d.data).map(d => {
                const seriesName = hideSeriesName ? ' ' : `${d.seriesName} : `;
                return `<br>${colorSpan(d.color)}${seriesName}${d.data.toLocaleString()}`;
              }).join('');
              return `${name}${series}`;
            }
          },
          legend: {
            show: !(this.hideLegend === true),
            data: yAxis
          },
          toolbox: { show : false },
          calculable : true,
          xAxis : [ { type : 'value' } ],
          yAxis : [
            {
              type : 'category',
              data : this.category
            }
          ],
          series : _.concat([], barSeries, lineSeries)
      };
    },
    onDblclick(param) {
      if (param.componentType !== `series` || !param.seriesName || !this.dblEvent) return;
      this.$events.fire(this.dblEvent, { id: param.seriesName });
    }
  },
  watch: {
    bar: function() {
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