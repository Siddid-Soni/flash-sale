//! Entry point. Everything below `App` is loaded eagerly; checkout and polling
//! are a separate chunk fetched while the countdown runs.

import { render } from 'preact';
import { App } from './app';
import './styles.css';

const root = document.getElementById('app');
if (root === null) throw new Error('Missing #app element');

render(<App />, root);
