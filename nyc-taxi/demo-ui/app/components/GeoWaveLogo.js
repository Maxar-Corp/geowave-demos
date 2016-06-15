import React, {Component} from 'react'
import styles from './GeoWaveLogo.css'
import logo from '../images/geowave-logo.svg'

export default class GeoWaveLogo extends Component {
  render() {
    return (
      <img className={styles.root} src={logo}/>
    )
  }
}
