import { createRouter, createWebHistory } from 'vue-router'

import DeliveryManifestView from '@/views/DeliveryManifestView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/delivery', name: 'delivery', component: DeliveryManifestView },
    // /:moduleKey 是动态兜底，必须放在 /delivery 之后。
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
