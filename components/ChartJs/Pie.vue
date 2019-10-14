<script>
import { Pie } from 'vue-chartjs';
import 'chartjs-plugin-colorschemes';
import 'chartjs-plugin-datalabels';

export default {
  extends: Pie,
  props: ['data', 'hideLabels'],
  methods: {
    renderPieChart() {
      this.renderChart(this.data, {
        legend: {
          display: false
        },
        plugins: {
          colorschemes: {
            scheme: 'tableau.Tableau20'
          },
          datalabels: {
            display: this.hideLabels === true ? false : true,
            anchor: 'end',
            align: 'start',
            formatter: (value, ctx) => {
                let sum = 0;
                const dataArr = ctx.chart.data.datasets[0].data;
                dataArr.forEach(data => sum += data);
                const percentage = (value*100 / sum).toFixed(2);
                return `${ctx.chart.data.labels[ctx.dataIndex]}\n${percentage}%`;
            },
            color: '#fff',
          }
        }
      });
    }
  },
  watch: {
    data: function() {
      this.renderPieChart();
    }
  },
  mounted() {
    this.renderPieChart();
  }
}
</script>