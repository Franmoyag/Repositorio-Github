import { Pressable, Text, StyleSheet } from "react-native"


interface Props {
    label: string;
    onPress?: () => void;
}


export const PrimaryButton = ({label, onPress}:Props) => {
    return (
        <Pressable
        onPress={() => onPress && onPress()}
        style={ ({pressed}) => [
        styles.button,
        pressed && styles.buttonPressed
       ]}
       >
         <Text style={{color: 'white'}}>
            {label}
            </Text>
       </Pressable>
    )
}

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
    button: {
      backgroundColor: '#208dc0',
      paddingHorizontal: 40,
      paddingVertical: 10,
      borderRadius: 10,
    },
    buttonPressed: {
      backgroundColor: '#16506b'
    }
})