import React, {Component} from 'react'
import MapView from './MapView'
import GeoWaveLogo from './GeoWaveLogo'
import InfoObjectList from './InfoObjectList'
//import Button from './Button'
import styles from './Application.css'

export default class Application extends Component {
  constructor() {
    super()
    this.state = {info: [], origin: {lat: 40.78, lng: -73.96}, destination: {lat: 40.65, lng: -73.78}, infoVisible: false}
    this._destinationChanged = this._destinationChanged.bind(this)
    this._originChanged = this._originChanged.bind(this)
    this._fetchInfo = debounce(this._fetchInfo.bind(this), 500)
    this._setInfoVisible = this._setInfoVisible.bind(this)
  }

  componentDidMount() {
    this._fetchInfo()
  }

  render() {

    var dist = 0
    var fares = 0
    var tolls = 0
    var timestamp = ""
    var seconds = 0
    var ret =[]   
    var ret2 = []
   
    if(Object.values(this.state.info).length >0){

      timestamp = Object.values(this.state.info)[0][0][0]
      ret.push(timestamp)

      seconds = Object.values(this.state.info)[0][0][1]
      ret.push(seconds)

      var stats = Object.values(Object.values(this.state.info)[1])

      dist = Object.values(stats[5])[3]
      ret.push(dist)

      fares = Object.values(stats[10])[3]
      ret.push(fares)
    
      tolls = Object.values(stats[4])[3]
      if(tolls<1){
        tolls=0
      }
      ret.push(tolls)

      ret2.push(ret)
    }

    return (
      <main className={styles.root}>
        <MapView origin={this.state.origin}
                 destination={this.state.destination}
                 originChanged={this._originChanged}
                 destinationChanged={this._destinationChanged}
                 setInfoVisible={this._setInfoVisible}>
        </MapView>
        <InfoObjectList info={ret2}
                        infoVisible={this.state.infoVisible}>
        </InfoObjectList>
        <footer className={styles.footer}>
          <GeoWaveLogo infoVisible={this.state.infoVisible}/>
        </footer>

      </main>
    )
  }

  //
  // Internals



  _fetchInfo() {
    const {origin, destination} = this.state
    const now = new Date().toISOString()
    this.setState({info: []})
    fetch(`/nyc-taxi/tripInfoExtra?startLat=${origin.lat}&startLon=${origin.lng}&destLat=${destination.lat}&destLon=${destination.lng}&startTime=${now}`)
      .then(response => response.json())
      .then(info => {
        this.setState({info})
      })

  }

  //
  // Events
  //

  _destinationChanged(destination) {
    this.setState({destination})
    this._fetchInfo()
  }

  _originChanged(origin) {
    this.setState({origin})
    this._fetchInfo()
  }


  _setInfoVisible() {
    this.setState({infoVisible: !this.state.infoVisible})
}

}

function debounce(callback, ms) {
  let id
  return () => {
    clearTimeout(id)
    id = setTimeout(callback, ms)
  }
}
