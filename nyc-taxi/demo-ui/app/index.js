import './styles/index.css'

import React from 'react'
import {render} from 'react-dom'
import Application from './components/Application'

const target = document.createElement('div')
document.body.appendChild(target)
render(<Application/>, target)
