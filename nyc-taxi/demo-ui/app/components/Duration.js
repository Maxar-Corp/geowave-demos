import React, {Component} from 'react'
import moment from 'moment'
import styles from './Duration.css'

const TIME_FORMAT = 'h:mma'

export default class Duration extends Component {
  render() {
    const [timestamp, seconds] = this.props.duration
    const now = moment(timestamp, moment.ISO_8601)
    const future = moment().add(seconds, 'seconds')
    return (
      <li className={styles.root}>
        <span className={styles.duration}>~{future.fromNow(true)}</span>
        <span className={styles.timestamp}>
          <span className={styles.label}>Departing At</span>
          <svg className={styles.clock} viewBox="1 1 98 98">
            <path d="M52,48.7046998 L52,11 L48,11 L48,51 L48.0081411,51 L48,51.0141009 L77.2977379,67.9291577 L79.2977379,64.4650561 L52,48.7046998 Z M50,99 C77.0619527,99 99,77.0619527 99,50 C99,22.9380473 77.0619527,1 50,1 C22.9380473,1 1,22.9380473 1,50 C1,77.0619527 22.9380473,99 50,99 Z M50.5,93 C73.9721019,93 93,73.9721019 93,50.5 C93,27.0278981 73.9721019,8 50.5,8 C27.0278981,8 8,27.0278981 8,50.5 C8,73.9721019 27.0278981,93 50.5,93 Z" fillRule="evenodd"/>
          </svg>
          {now.format(TIME_FORMAT)}</span>
      </li>
    )
  }
}

Duration.propTypes = {
  duration: React.PropTypes.array.isRequired
}
