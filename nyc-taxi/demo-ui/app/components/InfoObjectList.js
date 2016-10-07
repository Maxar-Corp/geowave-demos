import React, {Component} from 'react'
import InfoObject from './InfoObject'
import styles from './InfoObjectList.css'

export default class InfoObjectList extends Component {
  render() {
    return (
      <ul className={styles.root}>
        {this.props.info.map(i => <InfoObject key={i[0]} info={i} infoVisible={this.props.infoVisible}/>)}
      </ul>
    )
  }
}

InfoObjectList.propTypes = {
  info: React.PropTypes.array,
  infoVisible: React.PropTypes.bool
}
