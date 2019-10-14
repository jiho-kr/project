<script>
import { Bubble } from 'vue-chartjs';
import 'chartjs-plugin-colorschemes';

export default {
  extends: Bubble,
  props: ['data'],
  methods: {
    renderBubbleChart() {
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
          datalabels: {
            display: false
          },
          colorschemes: {
            scheme: 'tableau.Tableau20'
          }
        }
      });
    }
  },
  watch: {
    data: function() {
      this.renderBubbleChart();
    }
  },
  mounted() {
    this.renderBubbleChart();
  }
}
</script>