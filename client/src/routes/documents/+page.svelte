<script lang="ts">
  import { MainLayout } from '$components/layout';
  import { Card } from '$ui/card';

  let activeMenu = $state('Documents');
</script>

<Card class="relative m-0 min-h-screen w-full max-w-none rounded-none border-0 bg-[#f9f9f7] px-[clamp(20px,4vw,56px)] py-6 shadow-none">
  <MainLayout bind:activeMenu />

  <main class="flex h-[calc(100vh-88px)] min-h-0 flex-col">
    <h1 class="mt-4 mb-8 shrink-0 text-[32px] font-bold tracking-normal text-[#111111]">Documents</h1>
    <div class="border-t border-[#e6e6e4] py-6">
      <p class="m-0 mb-1 text-base font-semibold">Document workspace</p>
      <p class="m-0 text-[13px] text-[#757573]">Upload, review, and manage source records here.</p>
    </div>
  </main>
</Card>
