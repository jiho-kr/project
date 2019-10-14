<script>
import { Doughnut } from 'vue-chartjs';
import 'chartjs-plugin-colorschemes';

export default {
  extends: Doughnut,
  props: ['data'],
  methods: {
    renderDoughnutChart() {
      this.renderChart(this.data, {
        responsive: true,
        maintainAspectRatio: false,
        parseTime : false,
        scales: {
          yAxes: [{
            ticks: {
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
          position: 'right',
          labels: {
            fontSize: 10
          }
        },
        plugins: {
          colorschemes: {
            scheme: 'tableau.Tableau20'
          }
        }
      });
    }
  },
  watch: {
    data: function() {
      this.renderDoughnutChart();
    }
  },
  mounted() {
    this.renderDoughnutChart();
  }
}
</script>