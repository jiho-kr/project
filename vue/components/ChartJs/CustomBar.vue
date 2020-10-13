<script>
import { Bar } from 'vue-chartjs';
import 'chartjs-plugin-colorschemes';
import 'chartjs-plugin-datalabels';

export default {
  extends: Bar,
  props: ['data', 'options'],
  methods: {
    renderBarChart() {
      this.renderChart(this.data, this.options);
    }
  },
  watch: {
    data: function() {
      this.renderBarChart();
    }
  },
  mounted() {
    this.renderBarChart();
  }
}
</script>
