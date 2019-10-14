<script>
import { Line } from 'vue-chartjs';
import { formatBytes } from '@/utils/utils';
import 'chartjs-plugin-colorschemes';

export default {
  extends: Line,
  props: ['data', 'colorscheme', 'legnedPosition'],
  methods: {
    renderLineChart() {
      this.renderChart(this.data, {
        responsive: true,
        maintainAspectRatio: false,
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
          position: this.legnedPosition || 'right',
          labels: {
            fontSize: 9
          },
          onClick: function(e, legendItem) {
            const index = legendItem.datasetIndex;
            const ci = this.chart;
            const hiddendStatus = ci.data.datasets.map((v, i) => ci.getDatasetMeta(i).hidden);

            if (hiddendStatus.length > 3 && hiddendStatus.filter(Boolean).length === 0) {
              ci.data.datasets.forEach((v, i) => {
                if (i === index) return;
                ci.getDatasetMeta(i).hidden = true;
              });
            } else {
              const meta = ci.getDatasetMeta(index);
              meta.hidden = meta.hidden === null ? true : null;
            }
            const result = ci.data.datasets.map((v, i) => ci.getDatasetMeta(i).hidden).filter(Boolean).length;
            if (result === hiddendStatus.length) {
              ci.data.datasets.forEach((v, i) => ci.getDatasetMeta(i).hidden = null);
            }
            ci.update();
          }
        },
        plugins: {
          datalabels: {
            display: false
          },
          colorschemes: {
            scheme: this.colorscheme || 'tableau.GreenGold20'
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