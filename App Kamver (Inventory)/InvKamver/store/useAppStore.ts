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

interface Product {
  code: string;
  title: string;
  quantity: number;
  comment: string;
}

export interface AppState {
  user: User | null;
  inventories: Inventory[];
  scannedData: string[];
  productData: string[];
  login: (email: string) => void;
  addInventory: (inventory: Inventory) => void;
  addInventoryData: (data: string) => void;
  addProduct: (data: string) => void;
}

export const useAppStore = create<AppState>((set) => ({
  user: null,
  inventories: [],
  scannedData: [],
  login: (email) => set(() => ({ user: { email, loggedIn: true } })),
  addInventory: (inventory) => set((state) => ({ inventories: [...state.inventories, inventory] })),
  addInventoryData: (data) => set((state) => ({ scannedData: [...state.scannedData, data] })),
  addProduct: (data) => set((state) => ({ productData: [...state.productData, data]}))
}));
