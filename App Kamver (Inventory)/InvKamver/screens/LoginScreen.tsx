//Screen de Login

import React, { useState } from 'react';
import { View, Text, TextInput, Button, StyleSheet, Alert, Pressable } from 'react-native';
import { StackNavigationProp } from '@react-navigation/stack';
import { RootStackParamList } from './types';
import { PrimaryButton } from '../components';



type LoginScreenNavigationProp = StackNavigationProp<RootStackParamList, 'Login'>;

type Props = {
  navigation: LoginScreenNavigationProp;
};

// Credenciales predeterminadas
const DEFAULT_USER = 'correo@correo.com';
const DEFAULT_PASSWORD = 'pass1234';

const LoginScreen: React.FC<Props> = ({ navigation }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');

  const handleLogin = () => {
    if (email === DEFAULT_USER && password === DEFAULT_PASSWORD) {
      navigation.navigate('Home');
    } else {
      Alert.alert('Error', 'Contacte al Administrador del Sistema.');
    }
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Inventarios Kamver</Text>
      <Text>Email:</Text>
      <TextInput
        style={styles.input}
        value={email}
        onChangeText={setEmail}
        keyboardType="email-address"
        autoCapitalize="none"
      />
      <Text>Password:</Text>
      <TextInput
        style={styles.input}
        value={password}
        onChangeText={setPassword}
        secureTextEntry
      />

      <PrimaryButton
        label = 'Login'
        onPress={handleLogin}
      />
      
      
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    padding: 20,
    alignItems: 'center'
  },
  title: {
    fontSize: 24,
    textAlign: 'center',
    marginBottom: 20,
  },
  input: {
    borderWidth: 1,
    borderColor: '#ccc',
    paddingHorizontal: 50,
    marginBottom: 10,
  },
});




export default LoginScreen;
