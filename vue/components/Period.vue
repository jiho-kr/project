<template>
 <div>
   <form class="form-inline" style="display: flex; justify-content: flex-left">
      <Datetime v-model="startDate" :type="dateType" input-class="form-control" placeholder="StartDate" :format="dateFormat"/>
      &nbsp;
      <Datetime v-model="endDate" :type="dateType" input-class="form-control"  placeholder="EndDate" :format="dateFormat"/>
      <b-nav class="ml-sm">
        <b-nav-item-dropdown class="settingsDropdown d-sm-down-none">
          <b-dropdown-group header="DateTime Based">
            <b-dropdown-item v-if="dropDownItems.includes('10min')" @click="set10Min">10 Minute ago</b-dropdown-item>
            <b-dropdown-item v-if="dropDownItems.includes('hour')" @click="setHour">Hour ago</b-dropdown-item>
            <b-dropdown-item v-if="dropDownItems.includes('day')" @click="setDay">Day ago</b-dropdown-item>
            <b-dropdown-item v-if="dropDownItems.includes('week')" @click="setWeek">Week ago</b-dropdown-item>
            <b-dropdown-item v-if="dropDownItems.includes('month')" @click="setMonth">Month ago</b-dropdown-item>
          </b-dropdown-group>
          <template v-if="dropDownItems.includes('milestone')">
            <b-dropdown-divider />
            <b-dropdown-group header="Milestones">
              <b-dropdown-item v-for="(milestone, idx) in milestones" :key="idx" @click="setMilestone(idx)">
                {{ milestone.name }}
              </b-dropdown-item>
            </b-dropdown-group>
          </template>
          <template v-if="dropDownItems.includes('custom')">
            <b-dropdown-divider />
            <b-dropdown-group header="Custom">
              <b-dropdown-item v-for="(info, idx) in customPeriod" :key="idx" @click="setPeriod(info.start, info.end)">
                {{ info.name }}
              </b-dropdown-item>
              <b-dropdown-divider />
              <b-dropdown-item @click="$refs['customSetting'].show()">
                Custom Setting
              </b-dropdown-item>
              <b-dropdown-item @click="getCustomPeriod()">
                Refresh
              </b-dropdown-item>
            </b-dropdown-group>
          </template>
        </b-nav-item-dropdown>
      </b-nav>
      <b-modal size="lg" ref="customSetting" hide-footer title="Custom Setting">
        <b-card border-variant="success" bg-variant="light" class="h-100 mb-0">
          <div class="d-flex flex-wrap justify-content-between">
            <div class="mt">
              <p class="text-muted mb-0 mr"><small>Name</small></p>
              <b-form-input v-model="newName" type="text" placeholder="MS17 Live"/>
            </div>
            <div class="mt">
              <p class="text-muted mb-0 mr"><small>Start</small></p>
              <Datetime v-model="newStartDate" type="datetime" input-class="form-control" placeholder="StartDate" format="yyyy.MM.dd HH:mm"/>
            </div>
            <div class="mt">
              <p class="text-muted mb-0 mr"><small>End</small></p>
              <Datetime v-model="newEndDate" type="datetime" input-class="form-control" placeholder="StartDate" format="yyyy.MM.dd HH:mm"/>
            </div>
            <b-button variant="success" class="btn-comment" @click="newPeriod">
              <span class="fa fa-plus"> ADD</span>
            </b-button>
          </div>
        </b-card>
        <b-card border-variant="secondary" bg-variant="light" class="h-100 mb-0">
          <table class="table">
            <thead>
              <tr>
                <th> Name </th>
                <th> Start </th>
                <th> End </th>
                <th/>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(info, idx) of customPeriod" :key="idx">
                <td> {{info.name}} </td>
                <td> {{dateToString(info.start)}} </td>
                <td> {{dateToString(info.end)}} </td>
                <td>
                  <b-button variant="outline-danger" class="p-1 px-3 btn-xs" @click="removeCustomPeriod(idx)">
                    <span class="fa fa-trash-o"/>
                  </b-button>
                </td>
              </tr>
            </tbody>
          </table>
        </b-card>
      </b-modal>
   </form>
 </div>
</template>

<script>
import _ from 'lodash';
import moment from 'moment';
import { Datetime } from 'vue-datetime';

export default {
  name: 'PeriodComponent',
  components: { Datetime },
  props: {
    start: {
      type: String,
      default: undefined
    },
    end: {
      type: String,
      default: undefined
    },
    dateType: {
      type: String,
      default: 'date'
    },
    dateFormat: {
      type: String,
      default: 'yyyy.MM.dd'
    },
    dropDownItems: {
      type: Array,
      default: function () {
        return ['10min', 'hour', 'day', 'week', 'month', 'milestone', 'custom'];
      }
    }
  },
  data() {
    return {
      startDate: null,
      endDate: null,
      milestones: [],
      customPeriod: [],
      newName: null,
      newStartDate: null,
      newEndDate: null
    }
  },
  methods: {
    getKpi() {
      this.$http.get(`/Jira/KPI`)
      .then((res) => {
        this.milestones = _.orderBy(_.get(res, 'data', []).reduce((milestones, kpi) => {
          const { _id, production, hardening } = kpi;
          if (!_.isEmpty(production)) {
            milestones.push(_.merge({name: `${_id} Production`}, production));
          }
          if (!_.isEmpty(hardening)) {
            milestones.push(_.merge({name: `${_id} Hardening`}, hardening))
          }
          return milestones;
        }, []), ['name'], ['desc']);
      });
    },
    set10Min() {
      this.setStartDate(-10, 'm');
    },
    setHour() {
      this.setStartDate(-1, 'h');
    },
    setDay() {
      this.setStartDate(-1, 'd');
    },
    setWeek() {
      this.setStartDate(-1, 'w');
    },
    setMonth() {
      this.setStartDate(-1, 'M');
    },
    setMilestone(idx) {
      const { startDate, endDate } = this.milestones[idx];
      this.startDate = startDate;
      this.endDate = endDate;
    },
    setStartDate(amount, unit) {
      this.startDate = moment(this.endDate).add(amount, unit).toISOString();
    },
    setPeriod(start, end) {
      this.startDate = start;
      this.endData = end;
    },
    getCustomPeriod() {
      this.$http.get(`/policy/customPeriod`)
      .then(({data}) => {
        this.customPeriod = _.get(data, 'opts', []);
      });
    },
    newPeriod() {
      if (!this.newName || !this.newStartDate || !this.newEndDate) {
        return alert('입력창을 모두 채웠는지 확인해주세요');
      }
      this.customPeriod.push({name: this.newName, start: this.newStartDate, end: this.newEndDate});
      this.saveCustomPeriod().then(() => {
        this.newName = this.newStartDate = this.newEndDate = null;
      })
    },
    removeCustomPeriod(idx) {
      if (!confirm('삭제 하시겠습니까?')) {
        return;
      }
      this.customPeriod.splice(idx, 1);
      this.saveCustomPeriod();
    },
    saveCustomPeriod() {
      return this.$http.put(`/policy/customPeriod`, this.customPeriod).then(() => {});
    },
    dateToString(str) {
      return moment(str).format('YYYY.MM.DD HH:mm');
    }
  },
  mounted() {
    this.startDate = moment(this.start).add(-10, 'm').toISOString();
    this.endDate = moment(this.end).toISOString();
    if (this.dropDownItems.includes('milestone')) {
      this.getKpi();
    }
    if (this.dropDownItems.includes('custom')) {
      this.getCustomPeriod();
    }
  },
  watch: {
    startDate: function() {
      this.$emit('input', { start: this.startDate, end: this.endDate });
    },
    endDate: function() {
      this.$emit('input', { start: this.startDate, end: this.endDate });
    }
  }
}
</script>
