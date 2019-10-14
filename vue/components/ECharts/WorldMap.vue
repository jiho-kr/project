<template>
  <div class="echarts">
    <IEcharts :option="option" theme="macarons"/>
  </div>
</template>

<script>
import _ from 'lodash';
import echarts from 'echarts';
import IEcharts from 'vue-echarts-v3/src/full.js'
import 'echarts/theme/macarons.js'

IEcharts.registerMap('world', require('./json/world.json'))

export default {
  components: { IEcharts },
  // data: { xx: [ value, header ] }
  props: [ 'data', 'geoCoordMap'],
  data () {
    return {
      option: {},
      summaryCcu: {}
    }
  },
  methods: {
    summaryData() {
      let fo3 = 0, fo4 = 0;
      _.forEach(this.data, (value, area) => {
        if (area.indexOf('TOTAL') !== -1 || area.indexOf('STAGE') !== -1) return;
        if (area.indexOf('FO3' ) !== -1 ) {
          fo3 += value[0];
        } else {
          fo4 += value[0];
        }
      });
      this.summaryCcu = { fo4, fo3 };
    },
    makeMapData() {
      return _.map(this.data, (ccuInfo, name) => {
        const value = [];
        const geoPosition = this.geoCoordMap[name];
        if (!geoPosition) return;
        value.push(...geoPosition);
        value.push(...ccuInfo);
        return { name, value };
      }).filter(Boolean);
    },
    initData () {
      this.summaryData();
      const areaCcu = _.map(this.data, (v, k) => {
        if (k.indexOf('TOTAL') !== -1 || k.indexOf('STAGE') !== -1) return;
        return { name: k, value: v[0]}
      }).filter(Boolean);
      const getSortRank = (a) => a.indexOf('STAGE') !== -1 ? 3 : ( (a.indexOf('FO3' ) !== -1 ) ? 2 : 1 );
      areaCcu.sort((a, b) => {
        const first = getSortRank(a.name) - getSortRank(b.name);
        if (first === 0) {
          return b.value - a.value;
        }
        return first;
      });
      this.option = {
        backgroundColor: new echarts.graphic.RadialGradient(0.8, 0.5, 0.45, [{
          offset: 0,
          color: '#4b5769'
        }, {
          offset: 1,
          color: '#fafbff'
        }]),
        tooltip: {
          trigger: 'item',
          formatter: function (params) {
            return (params.value[3] || params.seriesName) + '<br/>' + params.name + ' : ' + (params.value[2] || params.value).toLocaleString()
          }
        },
        toolbox: {
          show: false,
          left: 'right',
          iconStyle: {
            normal: {
              borderColor: '#ddd'
            }
          },
          feature: {},
          z: 202
        },
        brush: {
          geoIndex: 0,
          brushLink: 'all',
          inBrush: {
            opacity: 1,
            symbolSize: 14
          },
          outOfBrush: {
            color: '#000',
            opacity: 0.2
          },
          z: 10
        },
        geo: {
          map: 'world',
          silent: true,
          label: {
            emphasis: {
              show: false,
              areaColor: '#eee'
            }
          },
          itemStyle: {
            normal: {
              borderWidth: 0.2,
              borderColor: '#404a59'
            }
          },
          center: [30, 10],
          zoom: 1.6,
          left: '6%',
          top: 40,
          right: '6%',
          roam: true
        },
        grid: [{
          show: true,
          left: 0,
          right: 0,
          top: '100%',
          bottom: 0,
          borderColor: 'transparent',
          backgroundColor: '#404a59',
          z: 99
        }],
        series: [
          {
            type: 'effectScatter',
            coordinateSystem: 'geo',
            symbolSize: function(val) {
              let size = val[2] / 1800;
              if (size < 5) size = 5;
              if (size > 60) size = 60;
              return size;
            },
            symbolEffectOn: 'render',
            hoverAnimation: true,
            rippleEffect: {
              brushType: 'stroke'
            },
            data: this.makeMapData(),
            activeOpacity: 1,
            label: {
              normal: {
                formatter: (params) => {
                  return (params.data.value[2] || 0).toLocaleString();
                },
                position: 'bottom',
                show: true
              },
              emphasis: {
                show: true
              }
            },
            itemStyle: {
              normal: {
                borderColor: '#fff',
                color: function(params) {
                  if (params.name.indexOf('STAGE') !== -1) {
                    return '#808080';
                  } else if (params.name.indexOf('FO3' ) !== -1 ) {
                    return '#ffd400';
                  }
                  return '#32cd32';
                },
                shadowBlur: 10,
                shadowColor: '#333'
              }
            }
          },
          {
            name:'CCU By Service',
            type:'pie',
            center: [200, 200],
            radius: [0, 60],
            itemStyle: {
              normal: {
                label: {
                  position: 'inner',
                  formatter: (params) => { 
                    if (+params.percent < 5) return;
                    return `${params.name}\n${(+params.percent).toFixed(0)}%`;
                  }
                },
                labelLine: {
                  show: false
                }
              },
              emphasis: {
                label: {
                  show: true,
                  formatter: '{b}\n{d}%'
                }
              }
            },
            data: _.map(this.summaryCcu, (value, name ) => {
              return { value, name };
            })
          },
          {
            name:'CCU By Area',
            type:'pie',
            center: [200, 200],
            radius: [80, 110],
            data: areaCcu
          },
          {
            name:'FO4 Total',
            type: 'gauge',
            center: [350, '85%'],
            min: 0,
            max: this.summaryCcu.fo4 > 200000 ? this.summaryCcu.fo4 : 200000,
            splitNumber: 10,
            radius: 100,
            axisLine: {
              lineStyle: { 
                color: [[0.2, '#228b22'],[0.8, '#48b'],[1, '#ff4500']], 
                width: 8
              }
            },
            axisTick: {
              splitNumber: 10,
              length :12,
              lineStyle: {
                color: 'auto'
              }
            },
            axisLabel: {
              show: false,
              textStyle: {
                color: 'auto'
              }
            },
            splitLine: {
              show: true,
              length :30,
              lineStyle: {
                color: 'auto'
              }
            },
            pointer : {
              width : 5
            },
            title : {
              show : true,
              offsetCenter: [0, 30],
              textStyle: {
                color: '#32cd32',
                fontWeight: 'bolder'
              }
            },
            detail : {
              formatter: (v) => {
                return v.toLocaleString();
              },
              offsetCenter: [0, 50],
              textStyle: {
                color: '#32cd32',
                fontWeight: 'bolder',
                fontSize: 20
              }
            },
            data:[{value: this.summaryCcu.fo4 || 0, name: 'FO4'}
            ]
          },
          {
            name:'FO3 Total',
            type: 'gauge',
            center: [550, '87%'],
            min: 0,
            max: this.summaryCcu.fo3 > 200000 ? this.summaryCcu.fo3 : 200000,
            splitNumber: 10,
            radius: 80,
            axisLine: {
              lineStyle: { 
                color: [[0.2, '#228b22'],[0.8, '#48b'],[1, '#ff4500']], 
                width: 8
              }
            },
            axisTick: {
              splitNumber: 10,
              length :12,
              lineStyle: {
                color: 'auto'
              }
            },
            axisLabel: {
              show: false,
              textStyle: {
                color: 'auto'
              }
            },
            splitLine: {
              show: true,
              length :30,
              lineStyle: {
                color: 'auto'
              }
            },
            pointer : {
              width : 5
            },
            title : {
              show : true,
              offsetCenter: [0, 30],
              textStyle: {
                color: '#ffd400',
                fontWeight: 'bolder'
              }
            },
            detail : {
              formatter: (v) => {
                return v.toLocaleString();
              },
              offsetCenter: [0, 50],
              textStyle: {
                color: '#ffd400',
                fontWeight: 'bolder',
                fontSize: 20
              }
            },
            data:[{value: this.summaryCcu.fo3 || 0, name: 'FO3'}
            ]
          }
        ]
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
    height: 620px;
  }
</style>