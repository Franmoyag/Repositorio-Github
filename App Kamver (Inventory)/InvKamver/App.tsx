import React from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createStackNavigator } from '@react-navigation/stack';
import LoginScreen from './screens/LoginScreen';
import HomeScreen from './screens/HomeScreen';
import InventoryScreen from './screens/InventoryScreen';
import CaptureScreen from './screens/CaptureScreen';
import NewInventory from './screens/NewInventory'
import { RootStackParamList }   from './screens/types';
import { PaperProvider } from 'react-native-paper';
import IonIcon from 'react-native-vector-icons/Ionicons'
import { useAppStore, AppState } from './store/useAppStore';
import InventoriesList from './screens/InventoriesList';
import InventoryDetails from './screens/InventoryDetails';


const Stack = createStackNavigator<RootStackParamList>();

const App: React.FC = () => {
  const user = useAppStore((state: AppState) => state.user);

  return (
    <PaperProvider
      settings={{
        icon: (props) => <IonIcon {...props} />
      }}
    >
      <NavigationContainer>
        <Stack.Navigator initialRouteName={user ? "Home" : "Login"}>
          <Stack.Screen name="Login" component={LoginScreen} />
          <Stack.Screen name="Home" component={HomeScreen} />
          <Stack.Screen name="Inventory" component={InventoryScreen} />
          <Stack.Screen name="Capture" component={CaptureScreen} />
          <Stack.Screen name="NewInventory" component={NewInventory} />
          <Stack.Screen name="InventoriesList" component={InventoriesList} />
          <Stack.Screen name="InventoryDetails" component={InventoryDetails} />
        </Stack.Navigator>
      </NavigationContainer>
    </PaperProvider>
  );
};

export default App;