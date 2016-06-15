import React, {Component} from 'react'
import styles from './Mask.css'

export default class Mask extends Component {
  render() {
    return (
      <div className={`${styles.root} ${this.props.className} ${this.props.visible ? styles.reveal : ''}`}>
        <div className={styles.children}>{this.props.children}</div>
      </div>
    )
  }
}

Mask.propTypes = {
  children: React.PropTypes.object,
  className: React.PropTypes.string,
  visible: React.PropTypes.bool
}
