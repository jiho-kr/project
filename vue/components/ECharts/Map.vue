<template>
  <div id="bmap" class="map"/>
</template>
<script>
import _ from 'lodash';
import echarts from 'echarts';
import 'echarts/extension/bmap/bmap';
import 'echarts-bmap';

export default {
  components: { },
  name: 'Map',
  data () {
    return {
      chart: echarts.Echarts,
      bmap: null,
      option: {
        title: {
          text: "title"
        },
        series: [
          {
            type: "map",
            mapType: "china",
            selectedMode: "single"
          }
        ]
      }
    }
  },
  mounted() {
    this.loadBMap().then(BMap => this.test());
  },
  methods: {
    loadBMap() {
      const AK = 'sa5G89TNkeSBokOsrsV5HfY54cwhjrRQ';
      const BMap_URL = `https://api.map.baidu.com/api?v=2.0&ak=${AK}&s=1&callback=onBMapCallback`;
      return new Promise((resolve, reject) => {
        if (typeof BMap !== 'undefined') {
          resolve(BMap);
          return true;
        }
        window.onBMapCallback = function () {
          resolve(BMap);
        };

        let scriptNode = document.createElement('script');
        scriptNode.setAttribute('type', 'text/javascript');
        scriptNode.setAttribute('src', BMap_URL);
        document.body.appendChild(scriptNode);
      });
    },
    test() {
      var option = {
        bmap: {
            center: [120.13066322374, 30.240018034923],
            zoom: 14,
            roam: true,
            mapStyle: {}
        },
        series: [{
            type: 'scatter',
            coordinateSystem: 'bmap',
            data: [ [120, 30, 1] ]
        }]
      };
      this.chart = echarts.init(document.getElementById('bmap'));
      this.chart.setOption(option);
      this.bmap = this.chart.getModel().getComponent('bmap').getBMap();
      this.bmap.addControl(new BMap.MapTypeControl());
    }
  }
}
</script>

<style scoped>
.map {
  width: 100%;
  height: 100%;
}
</style>