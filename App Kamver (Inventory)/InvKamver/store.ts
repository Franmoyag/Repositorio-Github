// store.ts
import {create} from 'zustand';

// Define una interfaz para el estado
interface CounterState {
  count: number;
  increase: () => void;
  decrease: () => void;
  reset: () => void;
}

// Crea la tienda con Zustand
const useStore = create<CounterState>((set) => ({
  count: 0,
  increase: () => set((state) => ({ count: state.count + 1 })),
  decrease: () => set((state) => ({ count: state.count - 1 })),
  reset: () => set({ count: 0 }),
}));

export default useStore;