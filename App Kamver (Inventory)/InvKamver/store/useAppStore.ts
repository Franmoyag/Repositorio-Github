// useAppStore.ts

import { create } from 'zustand';

interface User {
  email: string;
  loggedIn: boolean;
}

interface Inventory {
  id: string;
  warehouse: string;
  date: string;
}

export interface AppState {
  user: User | null;
  inventories: Inventory[];
  scannedData: string[];
  login: (email: string) => void;
  addInventory: (inventory: Inventory) => void;
  addInventoryData: (data: string) => void;
}

export const useAppStore = create<AppState>((set) => ({
  user: null,
  inventories: [],
  scannedData: [],
  login: (email) => set(() => ({ user: { email, loggedIn: true } })),
  addInventory: (inventory) => set((state) => ({ inventories: [...state.inventories, inventory] })),
  addInventoryData: (data) => set((state) => ({ scannedData: [...state.scannedData, data] })),
}));
