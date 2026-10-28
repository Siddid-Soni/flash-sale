//! Entry point. The home page and the sale page (`App`) are loaded eagerly; checkout and polling
//! are a separate chunk fetched while the countdown runs.

import { render } from 'preact';
import { App, saleIdFromUrl } from './app';
import { Home } from './home';
import './styles.css';

const root = document.getElementById('app');
if (root === null) throw new Error('Missing #app element');

// `/?sale=<id>` is a sale page; anything else is the list of sales.
render(saleIdFromUrl() === null ? <Home /> : <App />, root);
