import React, {PropTypes} from 'react';
import { Link } from 'react-router';
import './SignUp.css';

const SignUpForm = ({
    onSubmit,
    onChange,
    errors,
}) => 
(
    <div className="container">
        <div className="card-pannel login-panel">
            <form class="col s12" action="/" onSubmit={onSubmit}>
                <h4 className="center-align">SignUp</h4>
                {errors.summary && <div className="row"><p className="error-message">{errors.summary}</p></div>}
                <div className="row">
                    <div className="input-field col s12">
                    <input className="validate" id="email" type="email" name="email" onChange={onChange}/>
                    <label htmlFor='email'>Email</label>
                    </div>
                </div>
                {errors.email && <div className="row"><p className="error-message">{errors.email}</p></div>}
                <div className="row">
                    <div className="input-field col s12">
                        <input className="validate" id="password" type="password" name="password" onChange={onChange}/>
                        <label htmlFor='password'>Password</label>
                    </div>
                </div>
                {errors.password && <div className="row"><p className="error-message">{errors.password}</p></div>}
                <div className="row">
                    <div className="input-field col s12">
                        <input id="confirm_password" type="password" name="confirm_password" className="validate" onChange={onChange}/>
                        <label htmlFor="confirm_password">Conform Password</label>
                    </div>
                </div>                
                <div className="row right-align">
                    <input type="submit" className="waves-effect waves-light btn indigo lighten-1" value='Sign Up'/>
                </div>
                <div className="row">
                    <p className="right-align"> Already have an account? <Link to="/signup">Login</Link></p>
                </div>
            </form>
        </div>
    </div>
);

SignUpForm.prototype = {
    onSubmit: PropTypes.func.isRequired,
    onChange: PropTypes.func.isRequired,
    errors: PropTypes.object.isRequired
}

export default SignUpForm;