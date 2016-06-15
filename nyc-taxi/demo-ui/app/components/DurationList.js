import React, {Component} from 'react'
import Duration from './Duration'
import styles from './DurationList.css'

export default class DurationList extends Component {
  render() {
    return (
      <ul className={styles.root}>
        {this.props.durations.map(d => <Duration key={d[0]} duration={d}/>)}
      </ul>
    )
  }
}

DurationList.propTypes = {
  durations: React.PropTypes.array
}
