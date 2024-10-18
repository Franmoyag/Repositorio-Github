import { create } from 'zustand';

// Definir la estructura de un producto
interface Product {
  code: string;
  title: string;
  quantity: number;
  unit: string;
}

// Definir la interfaz del estado de zustand
interface ProductStore {
  products: Product[];
  addProduct: (product: Product) => void;
  updateProduct: (code: string, quantity: number) => void;
  removeProduct: (code: string) => void;
  clearProducts: () => void;
}

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
  productData: string[];
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

// Crear el store usando zustand
const useProductStore = create<ProductStore>((set) => ({
  products: [],

  // Función para agregar un producto
  addProduct: (product) =>
    set((state) => ({
      products: [...state.products, product],
    })),

  // Función para actualizar un producto
  updateProduct: (code, updatedProduct) =>
    set((state) => ({
      products: state.products.map((product) =>
        product.code === code ? { ...product, ...updatedProduct } : product
      ),
    })),

  // Función para eliminar un producto
  removeProduct: (code) =>
    set((state) => ({
      products: state.products.filter((product) => product.code !== code),
    })),

  // Limpiar todos los productos
  clearProducts: () => set({ products: [] }),
}));

export default useProductStore;
