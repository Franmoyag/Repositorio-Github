import React from 'react';
import { NavigationContainer } from '@react-navigation/native';
import { createStackNavigator } from '@react-navigation/stack';
import LoginScreen from './screens/LoginScreen';
import HomeScreen from './screens/HomeScreen';
import InventoryScreen from './screens/InventoryScreen';
import CaptureScreen from './screens/CaptureScreen';
import { CameraView, CameraType, useCameraPermissions } from 'expo-camera';
import { RootStackParamList }   from './screens/types';
import { useState } from 'react'
import { View, Text } from 'react-native';

const Stack = createStackNavigator<RootStackParamList>();

const App: React.FC = () => {


  return (
    <NavigationContainer>
      <Stack.Navigator initialRouteName="Login">
        <Stack.Screen name="Login" component={LoginScreen} />
        <Stack.Screen name="Home" component={HomeScreen} />
        <Stack.Screen name="Inventory" component={InventoryScreen} />
        <Stack.Screen name="Capture" component={CaptureScreen} />
      </Stack.Navigator>
    </NavigationContainer>
  );
};

export default App;