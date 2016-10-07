import React, {Component} from 'react'
import styles from './GeoWaveLogo.css'
import logo from '../images/geowave-logo.svg'

export default class GeoWaveLogo extends Component {
  render() {
    return (
   		<div className={`${this.props.infoVisible? styles.hideLogo : ""}`}>
      		<img className={styles.root} src={logo}/>
    	</div>
    )
  }
}

GeoWaveLogo.propTypes = {
	infoVisible: React.PropTypes.bool.isRequired
}
