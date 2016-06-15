import 'leaflet-providers'
import '!style!css!../../node_modules/leaflet/dist/leaflet.css'
import React, {Component} from 'react'
import leaflet from 'leaflet'
import Mask from './Mask'
import destinationMarker from '../images/destination-marker.svg'
import originMarker from '../images/origin-marker.svg'
import styles from './MapView.css'

const ORIGIN_MARKER = leaflet.icon({
  iconUrl: originMarker,
  iconSize: [100, 100],
  iconAnchor: [50, 100]
})

const DEST_MARKER = leaflet.icon({
  iconUrl: destinationMarker,
  iconSize: [100, 100],
  iconAnchor: [50, 100]
})

export default class MapView extends Component {
  constructor() {
    super()
    this.markers = {origin: null, destination: null, line: null}
    this._located = this._located.bind(this)
    this._originChanged = this._originChanged.bind(this)
    this._destinationChanged = this._destinationChanged.bind(this)
    this._updateLinePosition = this._updateLinePosition.bind(this)
    this._neighborhoodClicked = this._neighborhoodClicked.bind(this)
  }

  componentDidUpdate() {
    this._updateMarkers()
  }

  componentDidMount() {
    this._initializeMap()
    this._fetchNeighborhoodOverlays()
    this._attachMarkers()
    this.map.on('locationfound', this._located)
    // this.map.locate()
  }

  render() {
    return (
      <div className={styles.root}>
        <div ref="container" className={styles.map}/>
        <Mask className={styles.mask} visible={!this.props.origin}>
          <div className={styles.locatingMessage}>
            <h2>Where are you anyway?</h2>
            Please wait while we find your location...
          </div>
        </Mask>
      </div>
    )
  }

  //
  // Internal API
  //

  _attachMarkers() {
    const {origin, destination} = this.props
    this.markers.origin = leaflet.marker(origin, {icon: ORIGIN_MARKER, draggable: true})
      .addTo(this.map)
      .on('drag', this._updateLinePosition)
      .on('dragend', this._originChanged)
    this.markers.destination = leaflet.marker(destination, {icon: DEST_MARKER, draggable: true})
      .addTo(this.map)
      .on('drag', this._updateLinePosition)
      .on('dragend', this._destinationChanged)
    this.markers.line = leaflet.polyline([origin, destination], {className: styles.flightPath})
      .addTo(this.map)
  }

  _fetchNeighborhoodOverlays() {
    fetch('/nyc-taxi/shapes/neighborhoods.geojson')
      .then(response => response.json())
      .then(geojson => {
        leaflet.geoJson(geojson, {
          onEachFeature: (_, layer) => layer.on('click', this._neighborhoodClicked),
          style(feature) {
            switch (feature.properties.borough) {
              case 'Manhattan': return {className: styles.manhattan}
              case 'Bronx': return {className: styles.bronx}
              case 'Brooklyn': return {className: styles.brooklyn}
              case 'Queens': return {className: styles.queens}
              case 'Staten Island': return {className: styles.statenIsland}
            }
          }
        }).addTo(this.map)
        this.markers.line.bringToFront()
      })
  }

  _initializeMap() {
    this.map = leaflet.map(this.refs.container, {
      center: [40.747777160820704, -73.9482879638672],
      zoom: 12,
      layers: [
        leaflet.tileLayer.provider('Stamen.TonerLite')
      ],
      maxBounds: [
        [41.10005163093046, -74.5147705078125],
        [40.31513750307456, -73.37493896484374]
      ],
      minZoom: 11
    })
    this.map.attributionControl.addAttribution(`Neighborhoods from <a href="http://catalog.opendata.city/organization/pediacities">CivicDashboards</a>`)
  }

  _updateMarkers() {
    const {origin, destination} = this.props
    this.markers.origin.setLatLng(origin)
    this.markers.destination.setLatLng(destination)
    this.markers.line.setLatLngs([origin, destination])
  }

  //
  // Events
  //

  _destinationChanged(event) {
    const {lat, lng} = event.target.getLatLng()
    this.props.destinationChanged({lat, lng})
  }

  _located({latlng: {lat, lng}}) {
    this.props.originChanged({lat, lng})
  }

  _neighborhoodClicked({latlng: {lat, lng}}) {
    this.props.destinationChanged({lat, lng})
  }

  _originChanged(event) {
    const {lat, lng} = event.target.getLatLng()
    this.props.originChanged({lat, lng})
  }

  _updateLinePosition() {
    const {origin, destination, line} = this.markers
    line.setLatLngs([origin.getLatLng(), destination.getLatLng()])
  }
}

MapView.propTypes = {
  className: React.PropTypes.string,
  destination: React.PropTypes.object,
  origin: React.PropTypes.object,
  destinationChanged: React.PropTypes.func,
  originChanged: React.PropTypes.func
}
