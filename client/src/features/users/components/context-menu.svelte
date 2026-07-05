<script lang="ts">
  import { Button } from '$ui/button';

  let isActive = $state(true);
</script>

<div class="absolute top-9 right-0 z-10 w-[220px] rounded-[14px] border border-black/4 bg-[#f2f2f0] p-1.5 shadow-[0_10px_25px_rgba(0,0,0,0.08)]">
  <div class="flex items-center justify-between px-3 py-2.5 text-[13px] font-medium">
    <span>Active</span>
    <label class="relative inline-block h-5 w-[34px]">
      <input class="peer size-0 opacity-0" type="checkbox" bind:checked={isActive} />
      <span class="absolute inset-0 cursor-pointer rounded-full bg-[#ccc] transition-colors duration-200 before:absolute before:bottom-[3px] before:left-[3px] before:size-3.5 before:rounded-full before:bg-white before:transition-transform before:duration-200 peer-checked:bg-[#1a1a1a] peer-checked:before:translate-x-3.5"></span>
    </label>
  </div>
  <div class="my-1 h-px bg-[#e3e3e0]"></div>
  <Button variant="ghost" class="flex h-auto w-full justify-start rounded-lg border-0 bg-transparent px-3 py-2.5 text-left text-[13px] font-medium text-[#111111] hover:bg-black/[0.03]">Edit profile</Button>
  <Button variant="ghost" class="flex h-auto w-full justify-start rounded-lg border-0 bg-transparent px-3 py-2.5 text-left text-[13px] font-medium text-[#111111] hover:bg-black/[0.03]">Reset password</Button>
  <div class="my-1 h-px bg-[#e3e3e0]"></div>
  <Button variant="ghost" class="flex h-auto w-full justify-start rounded-lg border-0 bg-transparent px-3 py-2.5 text-left text-[13px] font-medium text-[#c93b2b] hover:bg-black/[0.03]">Delete</Button>
</div>
