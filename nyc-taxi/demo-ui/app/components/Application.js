import React, {Component} from 'react'
import MapView from './MapView'
import GeoWaveLogo from './GeoWaveLogo'
import DurationList from './DurationList'
import styles from './Application.css'

export default class Application extends Component {
  constructor() {
    super()
    this.state = {durations: [], origin: {lat: 40.78, lng: -73.96}, destination: {lat: 40.65, lng: -73.78}}
    this._destinationChanged = this._destinationChanged.bind(this)
    this._originChanged = this._originChanged.bind(this)
    this._fetchDurations = debounce(this._fetchDurations.bind(this), 500)
  }

  componentDidMount() {
    this._fetchDurations()
  }

  render() {
    return (
      <main className={styles.root}>
        <MapView origin={this.state.origin}
                 destination={this.state.destination}
                 originChanged={this._originChanged}
                 destinationChanged={this._destinationChanged}>
        </MapView>
        <DurationList durations={this.state.durations}/>
        <footer className={styles.footer}>
          <GeoWaveLogo/>
        </footer>
      </main>
    )
  }

  //
  // Internals
  //

  _fetchDurations() {
    const {origin, destination} = this.state
    const now = new Date().toISOString()
    this.setState({durations: []})
    fetch(`/nyc-taxi/tripInfo?startLat=${origin.lat}&startLon=${origin.lng}&destLat=${destination.lat}&destLon=${destination.lng}&startTime=${now}`)
      .then(response => response.json())
      .then(durations => {
        this.setState({durations})
      })
  }

  //
  // Events
  //

  _destinationChanged(destination) {
    this.setState({destination})
    this._fetchDurations()
  }

  _originChanged(origin) {
    this.setState({origin})
    this._fetchDurations()
  }
}

function debounce(callback, ms) {
  let id
  return () => {
    clearTimeout(id)
    id = setTimeout(callback, ms)
  }
}
